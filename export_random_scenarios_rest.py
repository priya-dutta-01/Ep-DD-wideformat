import argparse
import os
import random
import re
import math
import time
import io
import itertools
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
from dotenv import load_dotenv


# ============================================================
# CONFIG KNOBS
# ============================================================

API_VER = "3.19"

# How many times we "reroll" a scenario (new random filter set) until the REFERENCE sheet returns data
MAX_SCENARIO_REROLLS = 20

# How many times we retry the SAME filter-set fetch (network hiccups / intermittent empties)
MAX_FETCH_ATTEMPTS = 3

# Parallelism (start conservative; bump if your Tableau Cloud allows it)
DEFAULT_MAX_WORKERS = 12

# Value selection rule:
# If True => for each randomly chosen filter, select >50% of its values (random k in [min_k..N])
# If False => selects 1–2 values (old behavior)
USE_MAJORITY_VALUE_SELECTION = True


# ============================================================
# Dashboard modules (easy to extend later)
# ============================================================

class DashboardModule:
    def __init__(self, dashboard_name: str):
        self.dashboard_name = dashboard_name

    def remove_from_random_pool(self) -> List[str]:
        return []

    def build_attempt_fixed_filters(self, rng: random.Random) -> Dict[str, List[str]]:
        return {}

    def validate_ready(self) -> None:
        return


class EpisodeDeepDiveModule(DashboardModule):
    PERIOD_FIELD = "period"  # Can be moved to mapping excel
    PRODUCT_FIELD = "product_name"  # Needs to be changed for different instruments
    EPISODE_FIELD = "episode_name"
    QUESTION_FIELD = "txt_question_long_prefix (group)"

    SURVEY_PROVIDER_FIELD = "survey_provider"
    COMPARISON_PROVIDER_FIELD = "comparison_provider"

    # NEW: provider-like parameters
    BRAND_INTEREST_FIELD = "brand_of_interest_para"
    BRAND_COMPARISON_FIELD = "brand_of_comparison_para"

    def __init__(self, dashboard_name: str, combo_pool: Dict[Tuple[str, str], List[str]]):
        super().__init__(dashboard_name)
        self.combo_pool = combo_pool

    def remove_from_random_pool(self) -> List[str]:
        return [
            self.PERIOD_FIELD,
            self.PRODUCT_FIELD,
            self.EPISODE_FIELD,
            self.QUESTION_FIELD,
            self.SURVEY_PROVIDER_FIELD,
            self.COMPARISON_PROVIDER_FIELD,
            self.BRAND_INTEREST_FIELD,
            self.BRAND_COMPARISON_FIELD,
        ]

    def validate_ready(self) -> None:
        if not self.combo_pool:
            raise ValueError(
                "deep_dive_questions combo_pool is empty. "
                "Please ensure the 'deep_dive_questions' sheet has valid rows."
            )

    def build_attempt_fixed_filters(self, rng: random.Random) -> Dict[str, List[str]]:
        fixed: Dict[str, List[str]] = {self.PERIOD_FIELD: ["Q1 2026"]}

        (product, question_long) = rng.choice(list(self.combo_pool.keys()))
        episodes = self.combo_pool[(product, question_long)] or []

        if not episodes:
            chosen_eps: List[str] = []
        else:
            k = 2 if len(episodes) >= 2 and rng.choice([True, False]) else 1
            chosen_eps = rng.sample(episodes, k=min(k, len(episodes)))

        fixed[self.PRODUCT_FIELD] = [product]
        fixed[self.QUESTION_FIELD] = [question_long]
        fixed[self.EPISODE_FIELD] = chosen_eps

        return fixed


def get_dashboard_module(
    dashboard_name: str,
    combo_pool: Optional[Dict[Tuple[str, str], List[str]]] = None
) -> DashboardModule:
    if str(dashboard_name).strip().lower() == "episode deep dive".lower():
        return EpisodeDeepDiveModule(dashboard_name, combo_pool=combo_pool or {})
    return DashboardModule(dashboard_name)


# ============================================================
# Helpers
# ============================================================

def normalize_view_name(sheet_name: str) -> str:
    return re.sub(r"[\s()]+", "", str(sheet_name)).strip()

def smart_split_values(values_str: str) -> list[str]:
    """
    Split filter-values into list items while preserving commas inside numbers like $25,000.

    Rules:
      1) If we see delimiters like ", $..." (common for income ranges), split on comma+space before '$'
      2) Otherwise split on commas that are NOT between digits (keeps 100,000 intact but splits 18-24, 25-34)
    """
    s = "" if values_str is None else str(values_str).strip()
    if not s:
        return []

    # Case 1: delimiters are ", " followed by "$"
    if re.search(r",\s+\$", s):
        parts = re.split(r",\s+(?=\$)", s)
    else:
        # Case 2: original behavior
        parts = re.split(r"(?<!\d),(?!\d)", s)

    return [p.strip() for p in parts if p.strip()]


def normalize_selected_values(vals: List[object]) -> List[object]:
    """
    Fix issue:
      if a selected value itself contains comma-separated items
      (e.g. "18-24, 25-34, 35-44"),
      expand it into multiple values so the WIDE explosion creates multiple rows.

    Important: uses smart_split_values, so "$100,000" will NOT get split.
    """
    out: List[object] = []
    for v in (vals or []):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue

        s = str(v).strip()
        if not s or s.lower() == "nan":
            continue

        if "," in s:
            parts = smart_split_values(s)
            if len(parts) > 1:
                out.extend(parts)
            else:
                out.append(s)
        else:
            out.append(s)

    # De-dupe while preserving order
    seen = set()
    uniq: List[object] = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)

    return uniq

def format_filter_selection(selected_filters: dict[str, list[str]]) -> str:
    return "; ".join([f"{k}=[{'|'.join(map(str, v))}]" for k, v in selected_filters.items()])

def build_vf_params(selected_filters: dict[str, list[str]]) -> dict[str, str]:
    params: Dict[str, str] = {}
    for field, vals in selected_filters.items():
        params[f"vf_{field}"] = ",".join([str(x) for x in vals if x is not None])
    return params

def safe_name(s: str) -> str:
    return re.sub(r"[^\w\-]+", "_", str(s)).strip("_")

def make_local_rng(seed: Optional[int], dashboard: str) -> random.Random:
    """
    Deterministic RNG per dashboard (NOT per provider) so scenarios are identical across providers.
    If seed is None, we still randomize per run.
    """
    base = f"{seed}|{dashboard}" if seed is not None else f"{dashboard}|{time.time_ns()}"
    r = random.Random()
    r.seed(base)
    return r


# ============================================================
# Tableau REST API helpers
# ============================================================

def tableau_sign_in_pat(server_url: str, site_content_url: str, pat_name: str, pat_secret: str, api_ver=API_VER):
    url = f"{server_url}/api/{api_ver}/auth/signin"
    payload = {
        "credentials": {
            "personalAccessTokenName": pat_name,
            "personalAccessTokenSecret": pat_secret,
            "site": {"contentUrl": site_content_url},
        }
    }
    r = requests.post(url, json=payload, headers={"Accept": "application/json"})
    r.raise_for_status()
    data = r.json()
    token = data["credentials"]["token"]
    site_id = data["credentials"]["site"]["id"]
    return token, site_id

def tableau_sign_out(server_url: str, token: str, api_ver=API_VER):
    url = f"{server_url}/api/{api_ver}/auth/signout"
    requests.post(url, headers={"X-Tableau-Auth": token})

def get_workbook_id(server_url: str, token: str, site_id: str, workbook_key: str, api_ver=API_VER) -> str:
    url = f"{server_url}/api/{api_ver}/sites/{site_id}/workbooks"
    headers = {"X-Tableau-Auth": token, "Accept": "application/json"}

    for field in ["contentUrl", "name"]:
        params = {"filter": f"{field}:eq:{workbook_key}", "pageSize": 1000, "pageNumber": 1}
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        wbs = r.json().get("workbooks", {}).get("workbook", [])
        if wbs:
            return wbs[0]["id"]

    raise ValueError(f"Workbook not found by contentUrl or name: {workbook_key}")

def list_views_in_workbook(server_url: str, token: str, site_id: str, workbook_id: str, api_ver=API_VER) -> list[dict]:
    url = f"{server_url}/api/{api_ver}/sites/{site_id}/workbooks/{workbook_id}/views"
    r = requests.get(url, headers={"X-Tableau-Auth": token, "Accept": "application/json"})
    r.raise_for_status()
    return r.json().get("views", {}).get("view", [])

def query_view_data_csv(server_url: str, token: str, site_id: str, view_id: str, vf_params: dict, api_ver=API_VER) -> pd.DataFrame:
    url = f"{server_url}/api/{api_ver}/sites/{site_id}/views/{view_id}/data"
    headers = {"X-Tableau-Auth": token, "Accept": "*/*"}

    r = requests.get(url, headers=headers, params=vf_params)
    r.raise_for_status()

    text = (r.text or "").strip()
    if not text:
        return pd.DataFrame()

    if text[:20].lower().startswith("<!doctype html") or "<html" in text[:200].lower():
        raise ValueError("Got HTML instead of CSV. Check permissions/auth or endpoint behavior.")

    try:
        return pd.read_csv(io.StringIO(text))
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


# ============================================================
# Random selection
# ============================================================

def pick_random_filters(filter_pool: Dict[str, List[str]], rng: random.Random) -> Dict[str, List[str]]:
    filter_names = list(filter_pool.keys())
    if not filter_names:
        return {}

    num_filters = rng.choice([1, 2])
    chosen_filters = rng.sample(filter_names, k=min(num_filters, len(filter_names)))

    selected: Dict[str, List[str]] = {}
    for f in chosen_filters:
        vals = filter_pool.get(f, [])
        if not vals:
            continue

        n = len(vals)

        if USE_MAJORITY_VALUE_SELECTION:
            min_k = (n // 2) + 1  # strictly > 50%
            k = rng.randint(min_k, n)
        else:
            k = rng.choice([1, 2])
            k = min(k, n)

        selected[f] = rng.sample(vals, k=k)

    return selected


def fetch_with_retry_same_filters(
    server_url: str,
    token: str,
    site_id: str,
    view_id: str,
    final_filters: Dict[str, List[str]],
    api_ver: str = API_VER,
) -> Tuple[pd.DataFrame, int, bool]:
    """
    Retry fetching the SAME vf params a few times (network flakiness).
    """
    for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
        try:
            vf_params = build_vf_params(final_filters)
            df = query_view_data_csv(server_url, token, site_id, view_id, vf_params, api_ver=api_ver)
            if df is not None and not df.empty:
                return df, attempt, True
        except Exception:
            time.sleep(0.25 * attempt)

    return pd.DataFrame(), MAX_FETCH_ATTEMPTS, False


# ============================================================
# deep_dive_questions loader
# ============================================================

def load_deep_dive_combo_pool(xls: pd.ExcelFile) -> Dict[Tuple[str, str], List[str]]:
    try:
        df = xls.parse("deep_dive_questions")
    except Exception:
        return {}

    df.columns = [str(c).strip().lower() for c in df.columns]
    req = {"product_name", "episode_name", "question_long"}
    if not req.issubset(df.columns):
        raise ValueError(f"deep_dive_questions must have columns: {req}")

    def clean_str(x) -> str:
        s = "" if x is None else str(x).strip()
        return "" if s.lower() == "nan" else s

    combo_pool: Dict[Tuple[str, str], List[str]] = {}
    for _, r in df.iterrows():
        product = clean_str(r.get("product_name"))
        episode = clean_str(r.get("episode_name"))
        question_long = clean_str(r.get("question_long"))
        if not product or not episode or not question_long:
            continue
        combo_pool.setdefault((product, question_long), []).append(episode)

    for k, eps in combo_pool.items():
        seen = set()
        out = []
        for e in eps:
            if e not in seen:
                seen.add(e)
                out.append(e)
        combo_pool[k] = out

    return combo_pool


# ============================================================
# providers loader
# ============================================================

def load_providers(xls: pd.ExcelFile) -> List[str]:
    try:
        df = xls.parse("providers")
    except Exception:
        return []

    df.columns = [str(c).strip().lower() for c in df.columns]
    col = None
    for candidate in ["provider", "survey_provider", "comparison_provider"]:
        if candidate in df.columns:
            col = candidate
            break
    if not col:
        raise ValueError("providers sheet must have a column: provider OR survey_provider OR comparison_provider")

    providers: List[str] = []
    for v in df[col].tolist():
        s = "" if v is None else str(v).strip()
        if s and s.lower() != "nan":
            providers.append(s)

    seen = set()
    out = []
    for p in providers:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


# ============================================================
# Scenario CSV (for Alteryx) in WIDE-EXPLODED format
# ============================================================

def scenario_filters_to_wide_exploded_rows(
    dashboard_name: str,
    provider: str,
    scen_num: int,
    scenario_id: str,
    reference_sheet: str,
    rerolls_used: int,
    final_filters: Dict[str, List[str]],
    all_filter_columns: List[str],
    exclude_filter_columns: Optional[set[str]] = None,
) -> List[Dict[str, object]]:
    """
    WIDE + EXPLODED:
      - fixed metadata cols
      - one col per filter in all_filter_columns
      - cartesian explosion across multi-valued filters
      - expand comma-separated "csv-in-a-cell" values into separate exploded rows
    """
    exclude_filter_columns = exclude_filter_columns or set()

    per_filter_values: List[List[object]] = []
    used_filter_cols: List[str] = []

    for f in all_filter_columns:
        if f in exclude_filter_columns:
            continue

        used_filter_cols.append(f)

        raw_vals = (final_filters or {}).get(f, None)
        if not raw_vals:
            per_filter_values.append([pd.NA])
            continue

        expanded = normalize_selected_values(raw_vals)

        if not expanded:
            per_filter_values.append([pd.NA])
        else:
            per_filter_values.append(expanded)

    rows: List[Dict[str, object]] = []
    for combo in itertools.product(*per_filter_values):
        row: Dict[str, object] = {
            "scenario_id": scenario_id,
            "dashboard_name": dashboard_name,
            "provider": provider,
            "scenario": int(scen_num),
            "reference_sheet": reference_sheet,
            "rerolls_used": int(rerolls_used),
        }
        for col, val in zip(used_filter_cols, combo):
            row[col] = val
        rows.append(row)

    return rows


# ============================================================
# Scenario generation ONCE per dashboard (same for all providers)
# ============================================================

def build_validated_scenarios_for_dashboard(
    server_url: str,
    token: str,
    site_id: str,
    reference_view_id: str,
    dashboard_name: str,
    gating_provider: str,
    scenarios: int,
    filter_pool: Dict[str, List[str]],
    module: DashboardModule,
    supports_survey_provider_on_ref: bool,
    supports_comparison_provider_on_ref: bool,
    supports_brand_interest_on_ref: bool,
    supports_brand_comparison_on_ref: bool,
    seed: Optional[int],
) -> Tuple[List[Dict[str, List[str]]], List[int]]:
    """
    Generate ONE frozen list of scenarios per dashboard (same for all providers),
    validated using a single gating_provider on the reference sheet.
    """
    rng = make_local_rng(seed, dashboard_name)

    scenario_filters: List[Dict[str, List[str]]] = []
    rerolls_used_list: List[int] = []

    for scen_num in range(1, scenarios + 1):
        got_data = False
        last_filters: Dict[str, List[str]] = {}

        for reroll in range(1, MAX_SCENARIO_REROLLS + 1):
            random_selected = pick_random_filters(filter_pool, rng)
            fixed_selected = module.build_attempt_fixed_filters(rng)

            final_filters: Dict[str, List[str]] = {}
            final_filters.update(random_selected)
            final_filters.update(fixed_selected)

            # Apply provider controls ONLY for gating on reference sheet (single provider)
            if supports_survey_provider_on_ref and hasattr(module, "SURVEY_PROVIDER_FIELD"):
                final_filters[module.SURVEY_PROVIDER_FIELD] = [gating_provider]
            if supports_comparison_provider_on_ref and hasattr(module, "COMPARISON_PROVIDER_FIELD"):
                final_filters[module.COMPARISON_PROVIDER_FIELD] = [gating_provider]

            if supports_brand_interest_on_ref and hasattr(module, "BRAND_INTEREST_FIELD"):
                final_filters[module.BRAND_INTEREST_FIELD] = [gating_provider]
            if supports_brand_comparison_on_ref and hasattr(module, "BRAND_COMPARISON_FIELD"):
                final_filters[module.BRAND_COMPARISON_FIELD] = [gating_provider]

            last_filters = final_filters

            df, _, ok = fetch_with_retry_same_filters(
                server_url, token, site_id, reference_view_id, final_filters
            )
            if ok:
                got_data = True
                scenario_filters.append(final_filters)
                rerolls_used_list.append(reroll)
                break

        if not got_data:
            scenario_filters.append(last_filters)
            rerolls_used_list.append(MAX_SCENARIO_REROLLS)

    # Strip provider controls from frozen base scenarios (so they are provider-agnostic)
    if hasattr(module, "SURVEY_PROVIDER_FIELD"):
        sp = module.SURVEY_PROVIDER_FIELD
        for d in scenario_filters:
            d.pop(sp, None)
    if hasattr(module, "COMPARISON_PROVIDER_FIELD"):
        cp = module.COMPARISON_PROVIDER_FIELD
        for d in scenario_filters:
            d.pop(cp, None)

    if hasattr(module, "BRAND_INTEREST_FIELD"):
        bi = module.BRAND_INTEREST_FIELD
        for d in scenario_filters:
            d.pop(bi, None)
    if hasattr(module, "BRAND_COMPARISON_FIELD"):
        bc = module.BRAND_COMPARISON_FIELD
        for d in scenario_filters:
            d.pop(bc, None)

    return scenario_filters, rerolls_used_list


# ============================================================
# Worker for parallel fetch (NO randomness here)
# ============================================================

def worker_fetch_one(
    server_url: str,
    token: str,
    site_id: str,
    view_id: str,
    dashboard_name: str,
    sheet_name: str,
    provider: str,
    scen_num: int,
    final_filters: Dict[str, List[str]],
) -> Dict:
    df, attempts_used, got_data = fetch_with_retry_same_filters(server_url, token, site_id, view_id, final_filters)
    return {
        "dashboard_name": dashboard_name,
        "sheet_name": sheet_name,
        "provider": provider,
        "scenario": scen_num,
        "filters": final_filters,
        "filter_selection": format_filter_selection(final_filters),
        "attempts_used": attempts_used,
        "got_data": got_data,
        "df": df,
    }


# ============================================================
# Main
# ============================================================

def main():
    load_dotenv(override=True)

    parser = argparse.ArgumentParser()
    parser.add_argument("--mapping_file", required=True)
    parser.add_argument("--workbook_name", required=True, help="Workbook DISPLAY name as it appears in Tableau")
    parser.add_argument("--scenarios", type=int, default=5)
    parser.add_argument("--out", default="exports")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--max_workers", type=int, default=DEFAULT_MAX_WORKERS)
    args = parser.parse_args()

    server_url = os.getenv("TABLEAU_SERVER_URL", "").strip()
    site = os.getenv("TABLEAU_SITE", "").strip()
    pat_name = os.getenv("TABLEAU_TOKEN_NAME", "").strip()
    pat_secret = os.getenv("TABLEAU_TOKEN_VALUE", "").strip()

    if not all([server_url, site, pat_name, pat_secret]):
        raise RuntimeError("Missing env vars: TABLEAU_SERVER_URL, TABLEAU_SITE, TABLEAU_TOKEN_NAME, TABLEAU_TOKEN_VALUE")

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    xls = pd.ExcelFile(args.mapping_file)
    mapping_df = xls.parse("dashboard_sheet_column_mapping")
    filters_df = xls.parse("filter_details")

    mapping_df.columns = [c.strip().lower() for c in mapping_df.columns]
    filters_df.columns = [c.strip().lower() for c in filters_df.columns]

    required_map = {"dashboard name", "sheet name", "column name", "generic column name"}
    required_fil = {"filter name", "filter values"}
    if not required_map.issubset(mapping_df.columns):
        raise ValueError(f"dashboard_sheet_column_mapping must have columns: {required_map}")
    if not required_fil.issubset(filters_df.columns):
        raise ValueError(f"filter_details must have columns: {required_fil}")

    combo_pool = load_deep_dive_combo_pool(xls)

    providers = load_providers(xls)
    if not providers:
        raise ValueError("No providers found in 'providers' sheet.")

    # providers sample (100% currently)
    k = max(1, math.ceil(1 * len(providers)))
    sampler_rng = random.Random(args.seed) if args.seed is not None else random.Random()
    providers_sample = sampler_rng.sample(providers, k=k) if len(providers) > 1 else providers

    print(f"📌 Providers total={len(providers)} | sampled(100%)={len(providers_sample)}")
    print("📌 Sampled providers:", providers_sample)

    # Global filter pool + universe of filter columns for wide scenario file
    global_filter_pool: Dict[str, List[str]] = {}
    all_filter_columns_set: set[str] = set()

    for _, row in filters_df.iterrows():
        fname = str(row["filter name"]).strip()
        vals_raw = row["filter values"]
        vals = smart_split_values(vals_raw)

        # Also handle pipe-delimited values
        if len(vals) == 1 and isinstance(vals_raw, str) and "|" in vals_raw:
            vals = [p.strip() for p in str(vals_raw).split("|") if p.strip()]

        if fname and vals:
            global_filter_pool[fname] = vals
            all_filter_columns_set.add(fname)

    if not global_filter_pool:
        raise ValueError("No filters found in filter_details.")

    # Exclude provider-pill filter columns (we already have 'provider' column)
    exclude_filter_columns = {
        "survey_provider",
        "comparison_provider",
        "brand_of_interest_para",
        "brand_of_comparison_para",
    }

    # Accumulate WIDE exploded scenario rows for Alteryx
    scenario_wide_rows: List[Dict[str, object]] = []

    token = None
    try:
        token, site_id = tableau_sign_in_pat(server_url, site, pat_name, pat_secret, api_ver=API_VER)
        workbook_id = get_workbook_id(server_url, token, site_id, args.workbook_name, api_ver=API_VER)
        views = list_views_in_workbook(server_url, token, site_id, workbook_id, api_ver=API_VER)

        view_lookup: Dict[str, str] = {}
        for v in views:
            nm = v.get("name", "")
            view_lookup[normalize_view_name(nm)] = v["id"]

        # Process each dashboard
        for dashboard_name, dash_group in mapping_df.groupby("dashboard name"):
            dash_group = dash_group.copy()

            module = get_dashboard_module(dashboard_name, combo_pool=combo_pool)
            module.validate_ready()

            # Add module fixed fields to wide output column universe
            for attr in ["PERIOD_FIELD", "PRODUCT_FIELD", "EPISODE_FIELD", "QUESTION_FIELD"]:
                if hasattr(module, attr):
                    all_filter_columns_set.add(getattr(module, attr))

            # Remove special filters from random pool
            filter_pool = dict(global_filter_pool)
            for f in module.remove_from_random_pool():
                filter_pool.pop(f, None)

            # Build sheet list for dashboard
            dash_sheet_colmap: Dict[str, Dict[str, str]] = {}
            for sheet, g in dash_group.groupby("sheet name"):
                sheet_name = str(sheet)
                dash_sheet_colmap[sheet_name] = dict(
                    zip(g["column name"].astype(str), g["generic column name"].astype(str))
                )

            # Pick a reference sheet
            reference_sheet = list(dash_sheet_colmap.keys())[0]
            ref_norm = normalize_view_name(reference_sheet)
            if ref_norm not in view_lookup:
                raise ValueError(
                    f"Reference sheet '{reference_sheet}' not found as a view. "
                    f"Tried normalized name '{ref_norm}'."
                )
            reference_view_id = view_lookup[ref_norm]

            print(f"\n🧩 Dashboard '{dashboard_name}': reference sheet = '{reference_sheet}'")

            safe_dash = safe_name(dashboard_name)
            excel_out = out_dir / f"{safe_dash}.xlsx"

            results_by_sheet: Dict[str, List[pd.DataFrame]] = {s: [] for s in dash_sheet_colmap.keys()}

            # Freeze filter columns list (stable ordering) for scenario file
            all_filter_columns = sorted(all_filter_columns_set)

            # Build the frozen scenarios ONCE per dashboard (same for all providers)
            gating_provider = providers_sample[0]
            scenario_filters_list, rerolls_used_list = build_validated_scenarios_for_dashboard(
                server_url=server_url,
                token=token,
                site_id=site_id,
                reference_view_id=reference_view_id,
                dashboard_name=dashboard_name,
                gating_provider=gating_provider,
                scenarios=args.scenarios,
                filter_pool=filter_pool,
                module=module,
                # IMPORTANT: don't infer "support" from output columns; use module capability
                supports_survey_provider_on_ref=hasattr(module, "SURVEY_PROVIDER_FIELD"),
                supports_comparison_provider_on_ref=hasattr(module, "COMPARISON_PROVIDER_FIELD"),
                supports_brand_interest_on_ref=hasattr(module, "BRAND_INTEREST_FIELD"),
                supports_brand_comparison_on_ref=hasattr(module, "BRAND_COMPARISON_FIELD"),
                seed=args.seed,
            )

            # ---- For each provider: output scenario wide + fetch data (scenarios same across providers)
            for provider in providers_sample:
                # (A) Scenario WIDE exploded rows
                for scen_num in range(1, args.scenarios + 1):
                    scenario_id = f"{safe_dash}|{safe_name(provider)}|{scen_num}"
                    scenario_wide_rows.extend(
                        scenario_filters_to_wide_exploded_rows(
                            dashboard_name=dashboard_name,
                            provider=provider,
                            scen_num=scen_num,
                            scenario_id=scenario_id,
                            reference_sheet=reference_sheet,
                            rerolls_used=rerolls_used_list[scen_num - 1],
                            final_filters=scenario_filters_list[scen_num - 1],
                            all_filter_columns=all_filter_columns,
                            exclude_filter_columns=exclude_filter_columns,
                        )
                    )

                # (B) Create fetch jobs across all sheets & scenarios
                jobs = []
                for sheet_name in dash_sheet_colmap.keys():
                    norm = normalize_view_name(sheet_name)
                    if norm not in view_lookup:
                        raise ValueError(
                            f"Could not find a matching view for sheet '{sheet_name}'. "
                            f"Tried normalized name '{norm}'."
                        )
                    view_id = view_lookup[norm]

                    for scen_num in range(1, args.scenarios + 1):
                        base_filters = scenario_filters_list[scen_num - 1]
                        final_filters = dict(base_filters)

                        # IMPORTANT FIX:
                        # Always apply provider controls for real fetches.
                        # (Tableau often needs these even if they are not output columns.)
                        if hasattr(module, "SURVEY_PROVIDER_FIELD"):
                            final_filters[module.SURVEY_PROVIDER_FIELD] = [provider]
                        if hasattr(module, "COMPARISON_PROVIDER_FIELD"):
                            final_filters[module.COMPARISON_PROVIDER_FIELD] = [provider]
                        if hasattr(module, "BRAND_INTEREST_FIELD"):
                            final_filters[module.BRAND_INTEREST_FIELD] = [provider]
                        if hasattr(module, "BRAND_COMPARISON_FIELD"):
                            final_filters[module.BRAND_COMPARISON_FIELD] = [provider]

                        jobs.append((view_id, sheet_name, provider, scen_num, final_filters))

                # (C) Parallel fetch jobs for this provider
                with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as ex:
                    future_map = {}
                    for (view_id, sheet_name, provider_x, scen_num, final_filters) in jobs:
                        fut = ex.submit(
                            worker_fetch_one,
                            server_url, token, site_id, view_id,
                            dashboard_name, sheet_name, provider_x, scen_num, final_filters
                        )
                        future_map[fut] = (sheet_name, provider_x, scen_num)

                    for fut in as_completed(future_map):
                        sheet_name, provider_x, scen_num = future_map[fut]
                        try:
                            res = fut.result()
                        except Exception as e:
                            print(f"❌ Error: Dashboard '{dashboard_name}' | Sheet '{sheet_name}' | "
                                  f"Provider '{provider_x}' | Scenario {scen_num}: {e}")
                            continue

                        if not res["got_data"]:
                            print(f"⚠️ No data: Dashboard '{dashboard_name}' | Sheet '{sheet_name}' | "
                                  f"Provider '{provider_x}' | Scenario {scen_num} | "
                                  f"filters: {res['filter_selection']}")

                        df = res["df"]
                        if df is None or df.empty:
                            continue

                        # Keep Tableau column headers as-is
                        df_mod = df.copy()

                        # Ensure standard context columns exist on every sheet
                        filters_used = res.get("filters", {}) or {}

                        def _pick(vals):
                            if vals is None:
                                return ""
                            if isinstance(vals, list):
                                if len(vals) == 0:
                                    return ""
                                if len(vals) == 1:
                                    return str(vals[0])
                                return ", ".join(str(x) for x in vals)
                            return str(vals)

                        product_val = _pick(filters_used.get(getattr(module, "PRODUCT_FIELD", "product_name")))
                        episode_val = _pick(filters_used.get(getattr(module, "EPISODE_FIELD", "episode_name")))
                        question_val = _pick(filters_used.get(getattr(module, "QUESTION_FIELD", "txt_question_long_prefix (group)")))

                        if "product" not in df_mod.columns:
                            df_mod["product"] = product_val
                        if "episode" not in df_mod.columns:
                            df_mod["episode"] = episode_val
                        if "txt_question_long" not in df_mod.columns:
                            df_mod["txt_question_long"] = question_val

                        # Tracking columns
                        df_mod["scenario"] = res["scenario"]
                        df_mod["filter_selection"] = res["filter_selection"]
                        df_mod["provider"] = res["provider"]

                        results_by_sheet[sheet_name].append(df_mod)

            # Write dashboard excel after all providers processed
            with pd.ExcelWriter(excel_out, engine="openpyxl") as writer:
                for sheet_name in dash_sheet_colmap.keys():
                    parts = results_by_sheet.get(sheet_name, [])
                    out_df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
                    out_df.to_excel(writer, sheet_name=str(sheet_name)[:31], index=False)
                    print(f"✅ Dashboard '{dashboard_name}' | Sheet '{sheet_name}' -> Excel tab written")

            print(f"\n📘 Created Excel for dashboard '{dashboard_name}': {excel_out}\n")

        # Write scenario file for Alteryx (WIDE-EXPLODED format)
        if scenario_wide_rows:
            scen_df = pd.DataFrame(scenario_wide_rows)

            meta_cols = ["scenario_id", "dashboard_name", "provider", "scenario", "reference_sheet", "rerolls_used"]
            other_cols = [c for c in scen_df.columns if c not in meta_cols]

            # Keep stable ordering: meta first, then filters alphabetically (as built)
            scen_df = scen_df[meta_cols + other_cols]

            scen_out = out_dir / "scenario_wide_exploded.csv"
            scen_df.to_csv(scen_out, index=False)
            print(f"🧾 Scenario (wide exploded) file written for Alteryx: {scen_out}")

    finally:
        if token:
            tableau_sign_out(server_url, token, api_ver=API_VER)


if __name__ == "__main__":
    main()
