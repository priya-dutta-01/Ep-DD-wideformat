import re
from pathlib import Path
from typing import Dict, Tuple, List, Optional

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill


# ============================================================
# CONFIG – EDIT IF PATHS CHANGE
# ============================================================

SCENARIO_CSV = Path(
    r"C:\Users\57948\OneDrive - Bain\Documents\Ep-DD-wideformat\exports\scenario_wide_exploded.csv"
)

TABLEAU_XLSX = Path(
    r"C:\Users\57948\OneDrive - Bain\Documents\Ep-DD-wideformat\exports\Episode_deep_dive.xlsx"
)

ALTERYX_CSV = Path(
    r"C:\Users\57948\OneDrive - Bain\Documents\Ep-DD-wideformat\exports\Episode_deep_dive_wideformat.csv"
)

MAPPING_XLSX = Path(
    r"C:\Users\57948\OneDrive - Bain\Documents\Ep-DD-wideformat\mapping_file.xlsx"
)

MAPPING_SHEET = "dashboard_sheet_column_mapping"

OUTPUT_XLSX = Path(
    r"C:\Users\57948\OneDrive - Bain\Documents\Ep-DD-wideformat\comparison_output.xlsx"
)

DETAIL_ROW_CAP = 100000


# ============================================================
# Helpers
# ============================================================

def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", str(s)).strip()


def _std_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_clean(c) for c in df.columns]
    return df


def _safe_sheet(name: str, suffix: str) -> str:
    base = re.sub(r"[\[\]\*\?/\\:]", "_", str(name)).strip()
    max_base = 31 - len(suffix)
    if max_base < 1:
        return suffix[:31]
    return f"{base[:max_base]}{suffix}"


def _safe_flag_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", str(s)).strip("_")


def _clean_and_round_series(s: pd.Series, dp: int) -> pd.Series:
    """
    Cleans numeric-like strings before rounding:
      - removes '%' if present
      - removes commas
      - coerces to numeric
      - rounds to dp
    Works even if some rows are plain numerics and some are strings like "12.3%".
    """
    if s is None:
        return s

    s2 = s.astype(str)

    s2 = (
        s2.str.replace("%", "", regex=False)
          .str.replace(",", "", regex=False)
          .str.strip()
    )

    num = pd.to_numeric(s2, errors="coerce")
    return num.round(dp)


def _try_numeric_for_sort(s: pd.Series) -> pd.Series:
    """
    For sorting: if a column looks numeric-ish, sort numerically; else lexicographically.
    Keeps NaN at the end by default behavior in pandas.
    """
    if s is None:
        return s
    coerced = pd.to_numeric(s, errors="coerce")
    if coerced.notna().any():
        return coerced
    return s.astype(str)


# ============================================================
# Mapping
# ============================================================

def _load_mapping() -> pd.DataFrame:
    m = pd.read_excel(MAPPING_XLSX, sheet_name=MAPPING_SHEET, dtype=str)
    m = _std_cols(m)

    required = [
        "sheet name",
        "column name",
        "alteryx_column_name",
        "is_join_key",
        "compare",
        "include_in_output",
        "round_dp",
    ]
    missing = [c for c in required if c not in m.columns]
    if missing:
        raise ValueError(f"Mapping missing columns: {missing}")

    for c in ["sheet name", "column name", "alteryx_column_name"]:
        m[c] = m[c].map(_clean)

    for c in ["is_join_key", "compare", "include_in_output"]:
        m[c] = (
            m[c]
            .astype(str)
            .str.strip()
            .str.upper()
            .replace({"YES": "Y", "TRUE": "Y", "NO": "N", "FALSE": "N", "NAN": ""})
        )

    m["round_dp"] = m["round_dp"].astype(str).replace({"nan": ""}).map(str.strip)

    m = m[
        (m["sheet name"] != "")
        & (m["column name"] != "")
        & (m["alteryx_column_name"] != "")
    ].copy()

    return m


def _pairs_for_sheet(mtab: pd.DataFrame):
    join_pairs: List[Tuple[str, str]] = []
    cmp_pairs: List[Tuple[str, str]] = []
    keep_pairs: List[Tuple[str, str]] = []
    round_map: Dict[Tuple[str, str], int] = {}

    for _, r in mtab.iterrows():
        pair = (r["column name"], r["alteryx_column_name"])

        if r["is_join_key"] == "Y":
            join_pairs.append(pair)
        if r["compare"] == "Y":
            cmp_pairs.append(pair)
        if (r["is_join_key"] == "Y") or (r["compare"] == "Y") or (r.get("include_in_output", "") == "Y"):
            keep_pairs.append(pair)

        dp = str(r.get("round_dp", "")).strip()
        if dp and dp.lower() != "nan":
            try:
                round_map[pair] = int(float(dp))
            except Exception:
                pass

    def dedup(xs):
        out, seen = [], set()
        for x in xs:
            if x not in seen:
                out.append(x)
                seen.add(x)
        return out

    join_pairs = dedup(join_pairs)
    cmp_pairs = [p for p in dedup(cmp_pairs) if p not in join_pairs]
    keep_pairs = dedup(keep_pairs)

    if not join_pairs:
        raise ValueError("No join keys (is_join_key=Y)")
    if not cmp_pairs:
        raise ValueError("No compare fields (compare=Y)")

    return join_pairs, cmp_pairs, keep_pairs, round_map


def _get_alteryx_col_for_tableau_field(mapping: pd.DataFrame, tableau_field: str) -> Optional[str]:
    tf = tableau_field.strip().lower()
    m2 = mapping.copy()
    m2["__col_lc"] = m2["column name"].astype(str).str.strip().str.lower()
    hits = m2[m2["__col_lc"] == tf]
    if hits.empty:
        return None
    return str(hits["alteryx_column_name"].dropna().iloc[0]).strip()


# ============================================================
# Scenario join to populate filter_selection
# ============================================================

def _build_filter_selection_from_scenarios(scen: pd.DataFrame) -> pd.DataFrame:
    scen = _std_cols(scen)

    required = {"provider", "scenario"}
    if not required.issubset(set(scen.columns)):
        raise ValueError(f"scenario_wide_exploded.csv must contain columns: {required}")

    meta_cols = {
        "scenario_id", "dashboard_name", "provider", "scenario",
        "reference_sheet", "rerolls_used"
    }
    filter_cols = [c for c in scen.columns if c not in meta_cols]

    def agg_unique(series: pd.Series) -> str:
        out = []
        seen = set()
        for v in series.tolist():
            if pd.isna(v):
                continue
            s = str(v).strip()
            if not s:
                continue
            if s not in seen:
                seen.add(s)
                out.append(s)
        return "|".join(out)

    rows = []
    for (prov, scen_num), g in scen.groupby(["provider", "scenario"], dropna=False):
        parts = []
        for c in filter_cols:
            joined = agg_unique(g[c])
            if joined:
                parts.append(f"{c}=[{joined}]")
        rows.append({
            "scen_provider": prov,
            "scen_scenario": scen_num,
            "filter_selection_from_scen": "; ".join(parts),
        })

    return pd.DataFrame(rows)


def _attach_filter_selection_from_scenarios(
    alteryx_df: pd.DataFrame,
    provider_alteryx_col: str,
    scenario_alteryx_col: str,
) -> pd.DataFrame:
    scen_raw = pd.read_csv(SCENARIO_CSV)
    scen_key = _build_filter_selection_from_scenarios(scen_raw)

    alteryx_df = _std_cols(alteryx_df)

    if provider_alteryx_col not in alteryx_df.columns:
        raise ValueError(f"Alteryx provider column not found: '{provider_alteryx_col}'")
    if scenario_alteryx_col not in alteryx_df.columns:
        raise ValueError(f"Alteryx scenario column not found: '{scenario_alteryx_col}'")

    out = alteryx_df.merge(
        scen_key,
        left_on=[provider_alteryx_col, scenario_alteryx_col],
        right_on=["scen_provider", "scen_scenario"],
        how="left",
    )

    if "filter_selection" not in out.columns:
        out["filter_selection"] = pd.NA

    mask = out["filter_selection"].isna() | (out["filter_selection"].astype(str).str.strip() == "")
    out.loc[mask, "filter_selection"] = out.loc[mask, "filter_selection_from_scen"]

    out = out.drop(columns=["scen_provider", "scen_scenario", "filter_selection_from_scen"], errors="ignore")
    return out


# ============================================================
# Output formatting (match highlighting)
# ============================================================

def format_match_cells_true_false(xlsx_path: Path):
    """
    In every *__detail sheet:
      - TRUE cells in __match columns => green
      - FALSE cells in __match columns => red
    """
    wb = load_workbook(xlsx_path)

    green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")

    for ws in wb.worksheets:
        if not ws.title.endswith("__detail"):
            continue

        # Find __match columns by header
        match_col_idxs = []
        for c_idx, cell in enumerate(ws[1], start=1):
            if isinstance(cell.value, str) and cell.value.endswith("__match"):
                match_col_idxs.append(c_idx)

        if not match_col_idxs:
            continue

        # Apply fill row-by-row
        for row in ws.iter_rows(min_row=2):
            for c_idx in match_col_idxs:
                cell = row[c_idx - 1]
                v = cell.value
                if v is True or (isinstance(v, str) and v.strip().upper() == "TRUE"):
                    cell.fill = green_fill
                elif v is False or (isinstance(v, str) and v.strip().upper() == "FALSE"):
                    cell.fill = red_fill

    wb.save(xlsx_path)


# ============================================================
# Sorting detail output
# ============================================================

def _pick_first_existing(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _sort_detail(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sort order (ASC), highest priority first:
      1) scenario__tableau
      2) scenario__alteryx
      3) provider
      4) CV.SurvBank.143.1.1
    """

    scen_t_col = _pick_first_existing(
        df, ["scenario__tableau", "scenario_tableau", "scenario"]
    )
    scen_a_col = _pick_first_existing(
        df, ["scenario__alteryx", "scenario_alteryx"]
    )
    provider_col = _pick_first_existing(
        df, ["provider", "provider__alteryx", "provider__tableau",
             "survey_provider", "comparison_provider"]
    )
    cv_col = _pick_first_existing(
        df, ["CV.SurvBank.143.1.1",
             "CV.SurvBank.143.1.1__alteryx",
             "CV.SurvBank.143.1.1__tableau"]
    )

    sort_cols = []
    if scen_t_col:
        sort_cols.append(scen_t_col)
    if scen_a_col and scen_a_col not in sort_cols:
        sort_cols.append(scen_a_col)
    if provider_col and provider_col not in sort_cols:
        sort_cols.append(provider_col)
    if cv_col and cv_col not in sort_cols:
        sort_cols.append(cv_col)

    if not sort_cols:
        return df

    df2 = df.copy()

    # numeric-aware sorting
    tmp_cols = []
    for i, c in enumerate(sort_cols):
        tmp = f"__sortkey_{i}"
        df2[tmp] = _try_numeric_for_sort(df2[c])
        tmp_cols.append(tmp)

    df2 = df2.sort_values(
        by=tmp_cols,
        ascending=True,
        kind="mergesort"   # stable sort
    )

    df2 = df2.drop(columns=tmp_cols, errors="ignore")
    return df2.reset_index(drop=True)



# ============================================================
# Main
# ============================================================

def main():
    mapping = _load_mapping()

    # Load Alteryx CSV (do NOT rename columns)
    alteryx_df = _std_cols(pd.read_csv(ALTERYX_CSV))

    # Find which Alteryx columns correspond to Tableau 'provider' and 'scenario'
    provider_alteryx_col = _get_alteryx_col_for_tableau_field(mapping, "provider")
    scenario_alteryx_col = _get_alteryx_col_for_tableau_field(mapping, "scenario")

    if scenario_alteryx_col is None:
        scenario_alteryx_col = "scenario"

    if provider_alteryx_col is None:
        hint = [c for c in alteryx_df.columns if "prov" in c.lower() or "bank" in c.lower()][:20]
        raise ValueError(
            "Could not find Tableau field 'provider' in mapping file (column name = provider). "
            f"Add a mapping row with column name='provider' and alteryx_column_name='<your provider col>'. "
            f"Provider-like columns found in Alteryx: {hint}"
        )

    # Attach scenario-derived filter_selection for Alteryx-only rows
    alteryx_df = _attach_filter_selection_from_scenarios(
        alteryx_df,
        provider_alteryx_col=provider_alteryx_col,
        scenario_alteryx_col=scenario_alteryx_col,
    )

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        for sheet in sorted(mapping["sheet name"].unique()):
            mtab = mapping[mapping["sheet name"] == sheet].copy()

            try:
                join_pairs, cmp_pairs, keep_pairs, round_map = _pairs_for_sheet(mtab)
            except Exception as e:
                pd.DataFrame([{"sheet": sheet, "error": str(e)}]).to_excel(
                    writer, sheet_name=_safe_sheet(sheet, "__CFGERR"), index=False
                )
                continue

            try:
                tab_df = _std_cols(pd.read_excel(TABLEAU_XLSX, sheet_name=sheet))
            except Exception as e:
                pd.DataFrame([{"sheet": sheet, "error": str(e)}]).to_excel(
                    writer, sheet_name=_safe_sheet(sheet, "__READERR"), index=False
                )
                continue

            required_both = set(join_pairs) | set(cmp_pairs)

            missing_t = {t for (t, a) in required_both if t not in tab_df.columns}
            missing_a = {a for (t, a) in required_both if a not in alteryx_df.columns}

            print(f"Sheet '{sheet}': missing tableau cols: {missing_t}, missing alteryx cols: {missing_a}")
            if missing_t or missing_a:
                pd.DataFrame([{
                    "sheet": sheet,
                    "missing_tableau_cols": ", ".join(sorted(missing_t)),
                    "missing_alteryx_cols": ", ".join(sorted(missing_a)),
                }]).to_excel(
                    writer, sheet_name=_safe_sheet(sheet, "__KEYERR"), index=False
                )
                continue

            tab_join = tab_df.copy()
            alt_join = alteryx_df.copy()

            jk_cols = []
            for i, (tcol, acol) in enumerate(join_pairs):
                jk = f"__jk{i}"
                tab_join[jk] = tab_join[tcol]
                alt_join[jk] = alt_join[acol]
                jk_cols.append(jk)

            merged = tab_join.merge(
                alt_join,
                on=jk_cols,
                how="outer",
                suffixes=("__tableau", "__alteryx"),
                indicator=True,
            )

            def _blank(s):
                return s.isna() | (s.astype(str).str.strip() == "")

            fs_t_col = "filter_selection__tableau" if "filter_selection__tableau" in merged.columns else (
                "filter_selection" if "filter_selection" in merged.columns else None
            )
            fs_a_col = "filter_selection__alteryx" if "filter_selection__alteryx" in merged.columns else None

            sc_col = None
            for cand in ["scenario", "scenario__tableau", "scenario__alteryx"]:
                if cand in merged.columns:
                    sc_col = cand
                    break

            prov_col = None
            for cand in ["provider", "provider__tableau", "provider__alteryx", "survey_provider", "comparison_provider"]:
                if cand in merged.columns:
                    prov_col = cand
                    break

            if fs_t_col and sc_col:
                src = merged.copy()
                src_nonblank = src[~_blank(src[fs_t_col])]

                if prov_col and prov_col in merged.columns:
                    key = [prov_col, sc_col]
                    lut = (
                        src_nonblank.groupby(key)[fs_t_col]
                        .first()
                        .to_dict()
                    )
                    if "filter_selection__tableau" in merged.columns:
                        m = _blank(merged["filter_selection__tableau"])
                        merged.loc[m, "filter_selection__tableau"] = merged.loc[m].apply(
                            lambda r: lut.get((r.get(prov_col), r.get(sc_col))), axis=1
                        )
                else:
                    lut = (
                        src_nonblank.groupby(sc_col)[fs_t_col]
                        .first()
                        .to_dict()
                    )
                    if "filter_selection__tableau" in merged.columns:
                        m = _blank(merged["filter_selection__tableau"])
                        merged.loc[m, "filter_selection__tableau"] = merged.loc[m, sc_col].map(lut)

                if "filter_selection__tableau" in merged.columns:
                    m2 = _blank(merged["filter_selection__tableau"])
                    if fs_a_col and fs_a_col in merged.columns:
                        merged.loc[m2, "filter_selection__tableau"] = merged.loc[m2, fs_a_col]
                    elif "filter_selection" in merged.columns:
                        merged.loc[m2, "filter_selection__tableau"] = merged.loc[m2, "filter_selection"]

            scenario_col = None
            for cand in ["scenario", "scenario__tableau", "scenario__alteryx"]:
                if cand in merged.columns:
                    scenario_col = cand
                    break

            if scenario_col and "filter_selection" in merged.columns:
                nonblank = merged[
                    merged["filter_selection"].notna()
                    & (merged["filter_selection"].astype(str).str.strip() != "")
                ].copy()

                scen_to_fs = (
                    nonblank.groupby(scenario_col)["filter_selection"]
                    .first()
                    .to_dict()
                )

                mask = merged["filter_selection"].isna() | (merged["filter_selection"].astype(str).str.strip() == "")
                merged.loc[mask, "filter_selection"] = merged.loc[mask, scenario_col].map(scen_to_fs)

            # Field compare + match flags (NO SUMMARY SHEETS WRITTEN)
            for (tcol, acol) in cmp_pairs:
                t_series = merged.get(tcol, merged.get(f"{tcol}__tableau"))
                a_series = merged.get(acol, merged.get(f"{acol}__alteryx"))

                if t_series is None:
                    t_series = pd.Series([None] * len(merged))
                if a_series is None:
                    a_series = pd.Series([None] * len(merged))

                dp = round_map.get((tcol, acol))
                if dp is not None:
                    t_cmp = _clean_and_round_series(t_series, dp)
                    a_cmp = _clean_and_round_series(a_series, dp)
                else:
                    t_cmp = t_series
                    a_cmp = a_series

                both_mask = merged["_merge"] == "both"
                eq = (t_cmp.fillna("__NA__") == a_cmp.fillna("__NA__"))
                eq = eq.where(both_mask, other=pd.NA)

                flag_col = f"{_safe_flag_name(tcol)}__match"
                merged[flag_col] = eq

            # ---------------- DETAIL ----------------
            detail_cols: List[str] = []

            if "filter_selection" in merged.columns:
                detail_cols.append("filter_selection")

            for (tcol, acol) in join_pairs:
                if tcol in merged.columns:
                    detail_cols.append(tcol)
                elif f"{tcol}__tableau" in merged.columns:
                    detail_cols.append(f"{tcol}__tableau")

                if acol in merged.columns:
                    detail_cols.append(acol)
                elif f"{acol}__alteryx" in merged.columns:
                    detail_cols.append(f"{acol}__alteryx")

            for (tcol, acol) in cmp_pairs:
                if tcol in merged.columns:
                    detail_cols.append(tcol)
                elif f"{tcol}__tableau" in merged.columns:
                    detail_cols.append(f"{tcol}__tableau")

                if acol in merged.columns:
                    detail_cols.append(acol)
                elif f"{acol}__alteryx" in merged.columns:
                    detail_cols.append(f"{acol}__alteryx")

                flag_col = f"{_safe_flag_name(tcol)}__match"
                if flag_col in merged.columns:
                    detail_cols.append(flag_col)

            for (tcol, acol) in keep_pairs:
                if (tcol, acol) in join_pairs or (tcol, acol) in cmp_pairs:
                    continue

                if tcol in merged.columns and tcol not in detail_cols:
                    detail_cols.append(tcol)
                elif f"{tcol}__tableau" in merged.columns and f"{tcol}__tableau" not in detail_cols:
                    detail_cols.append(f"{tcol}__tableau")

                if acol in merged.columns and acol not in detail_cols:
                    detail_cols.append(acol)
                elif f"{acol}__alteryx" in merged.columns and f"{acol}__alteryx" not in detail_cols:
                    detail_cols.append(f"{acol}__alteryx")

            seen = set()
            detail_cols = [c for c in detail_cols if not (c in seen or seen.add(c))]

            detail = merged.loc[:, detail_cols].copy()
            if len(detail) > DETAIL_ROW_CAP:
                detail = detail.head(DETAIL_ROW_CAP)

            # ✅ SORT as requested (ascending)
            detail = _sort_detail(detail)

            detail.to_excel(writer, sheet_name=_safe_sheet(sheet, "__detail"), index=False)


if __name__ == "__main__":
    main()
    format_match_cells_true_false(OUTPUT_XLSX)
