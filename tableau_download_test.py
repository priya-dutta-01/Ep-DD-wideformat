# tableau_download_test.py
# Single-call Tableau REST "view data CSV" downloader with MULTI-SELECT age filter.
# Assumes the workbook filter is NOT "Only Relevant Values" (e.g., set to All Values / All Values in Context).

import os
import sys
import requests
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

API_VER = "3.19"
NS = {"t": "http://tableau.com/api"}

# -----------------------------
# FORCE LOAD .env from script dir
# -----------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DOTENV_PATH = os.path.join(SCRIPT_DIR, ".env")
if not os.path.exists(DOTENV_PATH):
    raise RuntimeError(f".env file not found at: {DOTENV_PATH}")
load_dotenv(dotenv_path=DOTENV_PATH, override=True)


def must_env(k: str) -> str:
    v = os.getenv(k)
    if not v:
        raise RuntimeError(f"Missing env var: {k}\nChecked .env at: {DOTENV_PATH}")
    return v


def tableau_headers(token: str) -> dict:
    return {"X-Tableau-Auth": token}


def preview_body(text: str, limit: int = 1200) -> str:
    t = (text or "").replace("\r", "")
    return t[:limit] + ("\n...(truncated)" if len(t) > limit else "")


def signin(server_url: str, site_content_url: str, token_name: str, token_value: str):
    """
    Tableau REST sign-in using XML (compatible across Tableau Cloud/Server).
    Returns (auth_token, site_id).
    """
    url = f"{server_url}/api/{API_VER}/auth/signin"
    xml_payload = f"""<?xml version="1.0" encoding="UTF-8"?>
<tsRequest>
  <credentials personalAccessTokenName="{token_name}" personalAccessTokenSecret="{token_value}">
    <site contentUrl="{site_content_url}"/>
  </credentials>
</tsRequest>
""".encode("utf-8")

    r = requests.post(url, data=xml_payload, headers={"Content-Type": "application/xml"}, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(
            f"Sign-in failed HTTP {r.status_code}\nURL: {url}\nBody preview:\n{preview_body(r.text)}"
        )

    root = ET.fromstring(r.text)
    cred = root.find(".//t:credentials", NS)
    site_el = root.find(".//t:site", NS)

    if cred is None or "token" not in cred.attrib:
        raise RuntimeError(f"Could not find credentials token.\nBody preview:\n{preview_body(r.text)}")
    if site_el is None or "id" not in site_el.attrib:
        raise RuntimeError(f"Could not find site id.\nBody preview:\n{preview_body(r.text)}")

    return cred.attrib["token"], site_el.attrib["id"]


def signout(server_url: str, token: str):
    try:
        url = f"{server_url}/api/{API_VER}/auth/signout"
        requests.post(url, headers=tableau_headers(token), timeout=30)
    except Exception:
        pass


def find_workbook_id(server_url: str, token: str, site_id: str, workbook_name: str) -> str:
    url = f"{server_url}/api/{API_VER}/sites/{site_id}/workbooks"
    headers = tableau_headers(token)

    page = 1
    page_size = 100
    while True:
        r = requests.get(url, headers=headers, params={"pageNumber": page, "pageSize": page_size}, timeout=60)
        if r.status_code >= 400:
            raise RuntimeError(
                f"Workbooks list failed HTTP {r.status_code}\nBody preview:\n{preview_body(r.text)}"
            )

        root = ET.fromstring(r.text)
        for wb in root.findall(".//t:workbook", NS):
            if wb.attrib.get("name") == workbook_name:
                return wb.attrib["id"]

        pagination = root.find(".//t:pagination", NS)
        if pagination is None:
            break
        total = int(pagination.attrib.get("totalAvailable", "0"))
        if page * page_size >= total:
            break
        page += 1

    raise RuntimeError(f"Workbook not found (exact name match): {workbook_name}")


def find_view_id(server_url: str, token: str, site_id: str, workbook_id: str, view_name: str) -> str:
    url = f"{server_url}/api/{API_VER}/sites/{site_id}/workbooks/{workbook_id}/views"
    headers = tableau_headers(token)

    r = requests.get(url, headers=headers, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(
            f"Views list failed HTTP {r.status_code}\nBody preview:\n{preview_body(r.text)}"
        )

    root = ET.fromstring(r.text)
    for v in root.findall(".//t:view", NS):
        if v.attrib.get("name") == view_name:
            return v.attrib["id"]

    all_views = [v.attrib.get("name", "") for v in root.findall(".//t:view", NS)]
    raise RuntimeError(
        f"View not found (exact name match): {view_name}\nViews in workbook:\n - " + "\n - ".join(all_views)
    )


def build_vf_params_single_call(filters: dict) -> dict:
    """
    Build vf_ params for ONE request.
    Multi-select values are comma-separated (Tableau REST view filters).
    """
    vf = {}
    for k, vals in filters.items():
        if vals is None:
            continue
        if isinstance(vals, str):
            vals = [vals]
        vals = [str(x) for x in vals if str(x).strip() != ""]
        if not vals:
            continue
        vf[f"vf_{k}"] = ",".join(vals)
    return vf


def download_view_csv(server_url: str, token: str, site_id: str, view_id: str, vf_params: dict):
    url = f"{server_url}/api/{API_VER}/sites/{site_id}/views/{view_id}/data"
    r = requests.get(url, headers=tableau_headers(token), params=vf_params, timeout=120)

    request_url = r.url
    if r.status_code >= 400:
        raise RuntimeError(
            f"CSV download failed HTTP {r.status_code}\nURL: {request_url}\nBody preview:\n{preview_body(r.text)}"
        )

    txt = r.text or ""
    if "<html" in txt.lower() and "," not in txt[:3000]:
        raise RuntimeError(
            f"CSV download returned HTML (not CSV).\nURL: {request_url}\nBody preview:\n{preview_body(txt)}"
        )

    return txt, request_url


def main():
    server_url = must_env("TABLEAU_SERVER_URL").rstrip("/")
    site = must_env("TABLEAU_SITE")  # contentUrl; default site should be blank ""
    token_name = must_env("TABLEAU_TOKEN_NAME")
    token_value = must_env("TABLEAU_TOKEN_VALUE")

    workbook_name = sys.argv[1] if len(sys.argv) > 1 else "Book1"
    view_name = sys.argv[2] if len(sys.argv) > 2 else "episode_dd_across_firms (3)"

    # Your filters (multi-select age in one request)
    filters = {
        "Resp Age Category": ["18-24", "35-44", "65+", "45-54", "55-64", "25-34"],
        "period": ["Q1 2026"],
        "product_name": ["Credit Card"],
        "txt_question_long_prefix (group)": ["Channel - Agent - Recommendations"],
        "episode_name": ["Apply for a new credit card"],
        "survey_provider": ["RBC"],
        "comparison_provider": ["RBC"],
        "brand_of_interest_para": ["RBC"],
        "brand_of_comparison_para": ["RBC"],
    }

    vf_params = build_vf_params_single_call(filters)

    token = None
    try:
        token, site_id = signin(server_url, site, token_name, token_value)
        wb_id = find_workbook_id(server_url, token, site_id, workbook_name)
        view_id = find_view_id(server_url, token, site_id, wb_id, view_name)

        print(f"Workbook: {workbook_name} -> {wb_id}")
        print(f"View:     {view_name} -> {view_id}")

        print("\nvf params:")
        for k, v in vf_params.items():
            print(f"  {k}={v}")

        csv_text, request_url = download_view_csv(server_url, token, site_id, view_id, vf_params)

        print("\nREQUEST URL:")
        print(request_url)

        lines = csv_text.splitlines()
        print("\n--- FIRST 50 LINES ---")
        for i, line in enumerate(lines[:50], start=1):
            print(f"{i:02d}: {line}")

        out = "tableau_check.csv"
        with open(out, "w", encoding="utf-8-sig", newline="") as f:
            f.write(csv_text)

        print(f"\nSaved CSV -> {out}")
        print(f"Total lines: {len(lines)}")

    finally:
        if token:
            signout(server_url, token)


if __name__ == "__main__":
    main()
