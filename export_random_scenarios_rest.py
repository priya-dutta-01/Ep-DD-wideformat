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
        # Base module has no enforced fixed filters
        return {}

    def validate_ready(self) -> None:
        return


class EpisodeDeepDiveModule(DashboardModule):
    """
    Fully mapping-driven: NO hardcoded field names.
    If a role is missing in semantic_field_mapping, we raise an error.
    """

    # No defaults (force mapping file)
    PERIOD_FIELD: str = ""
    PRODUCT_FIELD: str = ""
    EPISODE_FIELD: str = ""
    QUESTION_FIELD: str = ""

    SURVEY_PROVIDER_FIELD: str = ""
    COMPARISON_PROVIDER_FIELD: str = ""
    BRAND_INTEREST_FIELD: str = ""
    BRAND_COMPARISON_FIELD: str = ""

    def __init__(
        self,
        dashboard_name: str,
        combo_pool: Dict[Tuple[str, str], List[str]],
        semantic_cfg: Optional[Dict[str, object]] = None,
    ):
        super().__init__(dashboard_name)
        self.combo_pool = combo_pool

        semantic_cfg = semantic_cfg or {}
        field_by_role = (semantic_cfg.get("field_by_role") or {})

        # optional metadata from semantic sheet
        self.provider_control_fields: List[str] = list(semantic_cfg.get("provider_control_fields") or [])
        self.single_select_fields: List[str] = list(semantic_cfg.get("single_select_fields") or [])

        role_to_attr = {
            "period": "PERIOD_FIELD",
            "product": "PRODUCT_FIELD",
            "episode": "EPISODE_FIELD",
            "question": "QUESTION_FIELD",
            "provider_survey": "SURVEY_PROVIDER_FIELD",
            "provider_comparison": "COMPARISON_PROVIDER_FIELD",
            "brand_interest": "BRAND_INTEREST_FIELD",
            "brand_comparison": "BRAND_COMPARISON_FIELD",
        }

        # Apply semantic field mapping (role -> tableau field)
        for role, attr in role_to_attr.items():
            tf = str(field_by_role.get(role, "")).strip()
            if tf:
                setattr(self, attr, tf)

        # If provider_control_fields isn't explicitly provided,
        # infer it from whatever provider-like roles exist in mapping.
        if not self.provider_control_fields:
            inferred = []
            for f in [
                self.SURVEY_PROVIDER_FIELD,
                self.COMPARISON_PROVIDER_FIELD,
                self.BRAND_INTEREST_FIELD,
                self.BRAND_COMPARISON_FIELD,
            ]:
                if str(f).strip():
                    inferred.append(f)
            self.provider_control_fields = inferred

    def validate_ready(self) -> None:
        """
        Fail fast if semantic_field_mapping is missing required roles.
        """
        required_roles = {
            "period": self.PERIOD_FIELD,
            "product": self.PRODUCT_FIELD,
            "episode": self.EPISODE_FIELD,
            "question": self.QUESTION_FIELD,
        }

        missing = [role for role, field in required_roles.items() if not str(field).strip()]
        if missing:
            raise ValueError(
                f"[{self.dashboard_name}] semantic_field_mapping is missing required roles: {missing}. "
                "Add rows with role + tableau_field for these roles."
            )

    def remove_from_random_pool(self) -> List[str]:
        """
        Keep these controlled as single-select and always-present.
        """
        # period is always forced in code; we can remove it from randomness too if you want
        return [self.PRODUCT_FIELD, self.EPISODE_FIELD, self.QUESTION_FIELD]

    def build_attempt_fixed_filters(self, rng: random.Random) -> Dict[str, List[str]]:
        """
        Pick ONE product, ONE question, and ONE episode.
        combo_pool keys: (product, question) -> [episodes]
        """
        fixed: Dict[str, List[str]] = {}
        if not self.combo_pool:
            return fixed

        pq_keys = [k for k in self.combo_pool.keys() if isinstance(k, tuple) and len(k) == 2]
        if not pq_keys:
            return fixed

        product, question = rng.choice(pq_keys)
        fixed[self.PRODUCT_FIELD] = [product]
        fixed[self.QUESTION_FIELD] = [question]

        episodes = self.combo_pool.get((product, question), [])
        if episodes:
            fixed[self.EPISODE_FIELD] = [rng.choice(episodes)]

        return fixed


def _dash_key(s: str) -> str:
    # normalize: keep only alnum, lowercase
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())


def get_dashboard_module(
    dashboard_name: str,
    combo_pool: Optional[Dict[Tuple[str, str], List[str]]] = None,
    semantic_cfg: Optional[Dict[str, object]] = None,
) -> DashboardModule:
    # Match "Episode deep dive", "Episode_deep_dive", "EPISODE DEEP DIVE", etc.
    if _dash_key(dashboard_name) == _dash_key("Episode deep dive"):
        return EpisodeDeepDiveModule(dashboard_name, combo_pool=combo_pool or {}, semantic_cfg=semantic_cfg)
    return DashboardModule(dashboard_name)


# ============================================================
# Helpers
# ============================================================

def normalize_view_name(sheet_name: str) -> str:
    return re.sub(r"[\s()]+", "", str(sheet_name)).strip()


def format_filter_selection(selected_filters: dict[str, list[str]]) -> str:
    return "; ".join([f"{k}=[{'|'.join(map(str, v))}]" for k, v in selected_filters.items()])


def _strip_outer_quotes(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    if len(s) >= 2 and ((s[0] == '"' and s[-1] == '"') or (s[0] == "'" and s[-1] == "'")):
        s = s[1:-1].strip()
    s = s.replace('""', '"')
    return s


def parse_filter_values(values_str: str) -> list[str]:
    """
    Source of truth: mapping_file stores values pipe-delimited: A|B|C
    Fallback: split commas not between digits (keeps 25,000 intact)
    """
    s = "" if values_str is None else str(values_str).strip()
    if not s:
        return []

    if "|" in s:
        parts = [p.strip() for p in s.split("|")]
    else:
        parts = [p.strip() for p in re.split(r"(?<!\d),(?!\d)", s)]

    return [_strip_outer_quotes(p) for p in parts if p]


def normalize_selected_values(vals: List[object]) -> List[object]:
    """
    Expand multi-select strings into individual member values.
    """
    out: List[object] = []
    for v in (vals or []):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue

        s = str(v).strip()
        if not s or s.lower() == "nan":
            continue

        if "|" in s:
            out.extend(parse_filter_values(s))
            continue

        if "," in s:
            parts = parse_filter_values(s)
            if len(parts) > 1:
                out.extend(parts)
            else:
                out.append(parts[0] if parts else s)
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


def vf_encode_value(v: str, force_quote: bool = False) -> str:
    s = _strip_outer_quotes(v)
    if not s:
        return ""

    # Normalize curly apostrophes FIRST
    s = s.replace("’", "'").replace("‘", "'")

    needs_quote = force_quote or ("," in s) or ('"' in s)
    if needs_quote:
        s = s.replace('"', '""')   # escape quotes inside
        s = f'"{s}"'

    return s



def build_vf_params(selected_filters: dict[str, list[str]]) -> dict[str, str]:
    """
    Build Tableau vf params:
      vf_field=value1,value2,value3

    KEY FIX:
      If ANY member requires quoting (comma/quote), quote ALL members for that field.
    """
    params: Dict[str, str] = {}

    for field, vals in (selected_filters or {}).items():
        raw_vals = []
        for v in (vals or []):
            if v is None:
                continue
            vv = _strip_outer_quotes(str(v).strip())
            if vv:
                raw_vals.append(vv)

        if not raw_vals:
            continue

        # If any value contains a comma/quote, force-quote ALL values for this field
        force_quote_all = any(("," in x) or ('"' in x) for x in raw_vals)

        encoded = [vf_encode_value(x, force_quote=force_quote_all) for x in raw_vals]
        encoded = [e for e in encoded if e]

        if encoded:
            params[f"vf_{field}"] = ",".join(encoded)

    return params




def safe_name(s: str) -> str:
    return re.sub(r"[^\w\-]+", "_", str(s)).strip("_")


def make_local_rng(seed: Optional[int], dashboard: str) -> random.Random:
    """
    Deterministic RNG per dashboard (NOT per provider) so scenarios are identical across providers.
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
    print("REQUEST URL:", r.url)
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
            k = min(rng.choice([1, 2]), n)

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
            print("VF PARAMS:", vf_params)

            df = query_view_data_csv(server_url, token, site_id, view_id, vf_params, api_ver=api_ver)
            if df is not None and not df.empty:
                return df, attempt, True
        except Exception:
            time.sleep(0.25 * attempt)

    return pd.DataFrame(), MAX_FETCH_ATTEMPTS, False


# ============================================================
# Semantic fields loader
# ============================================================

def load_semantic_field_config(
    xls: pd.ExcelFile,
    dashboard_name: str,
    sheet_name: str = "semantic_field_mapping",
) -> Dict[str, object]:
    try:
        df = xls.parse(sheet_name)
    except Exception:
        return {"field_by_role": {}, "provider_control_fields": [], "single_select_fields": []}

    df.columns = [str(c).strip().lower() for c in df.columns]

    dash_col = "dashboard_name" if "dashboard_name" in df.columns else "dashboard name"
    role_col = "role"
    tab_col = "tableau_field" if "tableau_field" in df.columns else "tableau field"

    if dash_col not in df.columns or role_col not in df.columns or tab_col not in df.columns:
        raise ValueError(
            f"'{sheet_name}' must contain columns: dashboard_name (or dashboard name), role, tableau_field (or tableau field)"
        )

    dn = str(dashboard_name).strip().lower()
    sub = df[df[dash_col].astype(str).str.strip().str.lower() == dn].copy()

    def is_yes(x) -> bool:
        return str(x).strip().lower() in {"y", "yes", "true", "1"}

    field_by_role: Dict[str, str] = {}
    for _, r in sub.iterrows():
        role = str(r.get(role_col, "")).strip().lower()
        tf = str(r.get(tab_col, "")).strip()
        if role and tf:
            field_by_role[role] = tf

    provider_control_fields: List[str] = []
    if "is_provider_control" in sub.columns:
        for _, r in sub.iterrows():
            if is_yes(r.get("is_provider_control")):
                tf = str(r.get(tab_col, "")).strip()
                if tf:
                    provider_control_fields.append(tf)

    single_select_fields: List[str] = []
    if "required_single_select" in sub.columns:
        for _, r in sub.iterrows():
            if is_yes(r.get("required_single_select")):
                tf = str(r.get(tab_col, "")).strip()
                if tf:
                    single_select_fields.append(tf)

    def dedupe_keep_order(xs: List[str]) -> List[str]:
        seen = set()
        out = []
        for x in xs:
            if x and x not in seen:
                out.append(x)
                seen.add(x)
        return out

    return {
        "field_by_role": field_by_role,
        "provider_control_fields": dedupe_keep_order(provider_control_fields),
        "single_select_fields": dedupe_keep_order(single_select_fields),
    }


# ============================================================
# deep_dive_questions loader
# ============================================================

def load_deep_dive_combo_pool(xls: pd.ExcelFile) -> Dict[Tuple[str, str], List[str]]:
    try:
        df = xls.parse("deep_dive_questions")
    except Exception:
        return {}

    df.columns = [str(c).strip().lower() for c in df.columns]

    # Accept multiple possible headers
    product_col_candidates = ["product_name", "product_product_name", "product"]
    episode_col_candidates = ["episode_name", "episode_episode_name", "episode"]
    question_col_candidates = ["question_long", "question", "txt_question_long", "txt_question_long_prefix (group)"]

    def pick_col(cands):
        for c in cands:
            if c in df.columns:
                return c
        return None

    product_col = pick_col(product_col_candidates)
    episode_col = pick_col(episode_col_candidates)
    question_col = pick_col(question_col_candidates)

    if not product_col or not episode_col or not question_col:
        raise ValueError(
            "deep_dive_questions is missing required columns. "
            f"Found={list(df.columns)} | Need product in {product_col_candidates}, "
            f"episode in {episode_col_candidates}, question in {question_col_candidates}"
        )

    def clean_str(x) -> str:
        s = "" if x is None else str(x).strip()
        return "" if s.lower() == "nan" else s

    combo_pool: Dict[Tuple[str, str], List[str]] = {}
    for _, r in df.iterrows():
        product = clean_str(r.get(product_col))
        episode = clean_str(r.get(episode_col))
        question_long = clean_str(r.get(question_col))
        if not product or not episode or not question_long:
            continue
        combo_pool.setdefault((product, question_long), []).append(episode)

    # de-dupe episodes per key
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
        per_filter_values.append(expanded if expanded else [pd.NA])

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
    seed: Optional[int],
) -> Tuple[List[Dict[str, List[str]]], List[int]]:
    """
    Generate ONE frozen list of scenarios per dashboard (same for all providers),
    validated using a single gating_provider on the reference sheet.
    """
    rng = make_local_rng(seed, dashboard_name)

    scenario_filters: List[Dict[str, List[str]]] = []
    rerolls_used_list: List[int] = []

    def _first_value(field_name: str) -> Optional[str]:
        vals = filter_pool.get(field_name, [])
        return vals[0] if vals else None

    for scen_num in range(1, scenarios + 1):
        got_data = False
        last_filters: Dict[str, List[str]] = {}

        for reroll in range(1, MAX_SCENARIO_REROLLS + 1):
            random_selected = pick_random_filters(filter_pool, rng)
            fixed_selected = module.build_attempt_fixed_filters(rng)

            final_filters: Dict[str, List[str]] = {}
            final_filters.update(random_selected)
            final_filters.update(fixed_selected)

            # ALWAYS force period to be present (single select) if available in filter_pool.
            # Use semantic field name first; if not found, fall back to literal 'period'.
            period_field = getattr(module, "PERIOD_FIELD", "period")
            period_val = _first_value(period_field)
            if period_val is None and period_field != "period":
                period_val = _first_value("period")
                if period_val is not None:
                    period_field = "period"
            if period_val is not None:
                final_filters[period_field] = [period_val]

            # Enforce single-select fields (if any)
            single_fields = getattr(module, "single_select_fields", []) or []
            for f in single_fields:
                if f in final_filters and isinstance(final_filters[f], list) and len(final_filters[f]) > 1:
                    final_filters[f] = [final_filters[f][0]]

            # Apply provider controls for gating on reference sheet (single provider)
            provider_fields = getattr(module, "provider_control_fields", []) or []
            for f in provider_fields:
                final_filters[f] = [gating_provider]

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

    # Strip provider controls from frozen base scenarios (provider-agnostic)
    provider_fields = getattr(module, "provider_control_fields", []) or []
    for d in scenario_filters:
        for f in provider_fields:
            d.pop(f, None)

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
        vals = parse_filter_values(row["filter values"])
        if fname and vals:
            global_filter_pool[fname] = vals
            all_filter_columns_set.add(fname)

    if not global_filter_pool:
        raise ValueError("No filters found in filter_details.")

    # Base exclude list (will be extended dynamically per dashboard)
    base_exclude_filter_columns = {
        "survey_provider",
        "comparison_provider",
        "brand_of_interest_para",
        "brand_of_comparison_para",
    }

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

            semantic_cfg = load_semantic_field_config(xls, dashboard_name)
            module = get_dashboard_module(dashboard_name, combo_pool=combo_pool, semantic_cfg=semantic_cfg)
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

            # Dynamic exclude columns (provider controls, plus any hardcoded ones)
            exclude_filter_columns = set(base_exclude_filter_columns)
            for f in (getattr(module, "provider_control_fields", []) or []):
                exclude_filter_columns.add(f)

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

                        # Always apply provider controls for real fetches
                        provider_fields = getattr(module, "provider_control_fields", []) or []
                        for f in provider_fields:
                            final_filters[f] = [provider]

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

                        # IMPORTANT: use semantic_field_mapping-derived names ONLY (no hardcoded fallbacks)
                        product_field = module.PRODUCT_FIELD
                        episode_field = module.EPISODE_FIELD
                        question_field = module.QUESTION_FIELD

                        product_val = _pick(filters_used.get(product_field))
                        episode_val = _pick(filters_used.get(episode_field))
                        question_val = _pick(filters_used.get(question_field))

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
