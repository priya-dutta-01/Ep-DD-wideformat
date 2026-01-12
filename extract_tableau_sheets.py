# tableau_download_test.py
# Standalone Tableau REST "view data CSV" download tester that loads creds from .env

import os
import sys
import requests
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

API_VER = "3.19"


def load_env():
    """
    Loads .env from:
      1) TABLEAU_DOTENV_PATH if set, else
      2) .env in current working directory
    """
    dotenv_path = os.getenv("TABLEAU_DOTENV_PATH", ".env")
    load_dotenv(dotenv_path=dotenv_path, override=True)


def must_env(k: str) -> str:
    v = os.getenv(k)
    if not v:
        raise RuntimeError(
            f"Missing env var: {k}\n"
            f"Make sure {k} is present in your .env (or in system env vars).\n"
            f"Tip: you can set TABLEAU_DOTENV_PATH to point to the correct .env file."
        )
    return v


def tableau_headers(token: str) -> dict:
    return {"X-Tableau-Auth": token}


def signin(server_url: str, site_content_url: str, token_name: str, token_value: str):
    """
    Returns (auth_token, site_id)
    """
    url = f"{server_url}/api/{API_VER}/auth/signin"
    payload = {
        "credentials": {
            "personalAccessTokenName": token_name,
            "personalAccessTokenSecret": token_value,
            "site": {"contentUrl": site_content_url},
        }
    }
    r = requests.post(url, json=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    cred = data["credentials"]
    return cred["token"], cred["site"]["id"]


def signout(server_url: str, token: str):
    """
    Best-effort signout.
    """
    try:
        url = f"{server_url}/api/{API_VER}/auth/signout"
        requests.post(url, headers=tableau_headers(token), timeout=30)
    except Exception:
        pass


def find_workbook_id(server_url: str, token: str, site_id: str, workbook_name: str) -> str:
    """
    Searches workbooks by name (exact match) and returns workbook_id.
    Uses pagination.
    """
    url = f"{server_url}/api/{API_VER}/sites/{site_id}/workbooks"
    headers = tableau_headers(token)

    page = 1
    page_size = 100

    while True:
        r = requests.get(url, headers=headers, params={"pageNumber": page, "pageSize": page_size}, timeout=60)
        r.raise_for_status()

        root = ET.fromstring(r.text)
        ns = {"t": "http://tableau.com/api"}

        for wb in root.findall(".//t:workbook", ns):
            if wb.attrib.get("name") == workbook_name:
                return wb.attrib["id"]

        pagination = root.find(".//t:pagination", ns)
        if pagination is None:
            break

        total = int(pagination.attrib.get("totalAvailable", "0"))
        if page * page_size >= total:
            break
        page += 1

    raise RuntimeError(f"Workbook not found (exact name match): {workbook_name}")


def find_view_id(server_url: str, token: str, site_id: str, workbook_id: str, view_name: str) -> str:
    """
    Finds a view (sheet) inside a workbook by name (exact match) and returns view_id.
    """
    url = f"{server_url}/api/{API_VER}/sites/{site_id}/workbooks/{workbook_id}/views"
    headers = tableau_headers(token)

    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()

    root = ET.fromstring(r.text)
    ns = {"t": "http://tableau.com/api"}

    for v in root.findall(".//t:view", ns):
        if v.attrib.get("name") == view_name:
            return v.attrib["id"]

    # Helpful debug: list view names if not found
    all_views = [v.attrib.get("name", "") for v in root.findall(".//t:view", ns)]
    raise RuntimeError(
        f"View not found (exact name match): {view_name}\n"
        f"Views in workbook:\n - " + "\n - ".join(all_views)
    )


def build_vf_params(filters: dict) -> dict:
    """
    Converts {"Field": ["A","B"]} -> {"vf_Field": "A,B"}
    Tableau REST expects comma-separated values for multi-select.
    """
    vf = {}
    for k, vals in filters.items():
        if vals is None:
            continue
        if isinstance(vals, str):
            vals = [vals]
        vals = [str(x) for x in vals if str(x).strip() != ""]
        vf[f"vf_{k}"] = ",".join(vals)
    return vf


def download_view_csv(server_url: str, token: str, site_id: str, view_id: str, vf_params: dict) -> str:
    """
    Downloads CSV from /views/{view_id}/data with vf_ params.
    """
    url = f"{server_url}/api/{API_VER}/sites/{site_id}/views/{view_id}/data"
    headers = tableau_headers(token)

    r = requests.get(url, headers=headers, params=vf_params, timeout=120)
    r.raise_for_status()

    # Basic sanity: if Tableau returns an HTML error page, you'll often see <html
    txt = r.text or ""
    if "<html" in txt.lower() and "," not in txt[:2000]:
        raise RuntimeError(
            "Response looks like HTML (not CSV). "
            "This usually means auth/permissions or the endpoint returned an error page."
        )
    return txt


def main():
    # 1) load .env
    load_env()

    # 2) read creds from env
    server_url = must_env("TABLEAU_SERVER_URL").rstrip("/")
    site = must_env("TABLEAU_SITE")  # Tableau site contentUrl ("" for Default site)
    token_name = must_env("TABLEAU_TOKEN_NAME")
    token_value = must_env("TABLEAU_TOKEN_VALUE")

    # 3) workbook + view from CLI args (optional)
    workbook_name = sys.argv[1] if len(sys.argv) > 1 else "Book1"
    view_name = sys.argv[2] if len(sys.argv) > 2 else "episode_dd_across_firms (3)"

    # 4) Your exact filter settings
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
    vf_params = build_vf_params(filters)

    token = None
    try:
        # 5) sign in
        token, site_id = signin(server_url, site, token_name, token_value)

        # 6) resolve workbook + view ids
        wb_id = find_workbook_id(server_url, token, site_id, workbook_name)
        view_id = find_view_id(server_url, token, site_id, wb_id, view_name)

        print(f"Workbook: {workbook_name} -> {wb_id}")
        print(f"View:     {view_name} -> {view_id}")
        print("vf params:")
        for k, v in vf_params.items():
            print(f"  {k}={v}")

        # 7) download CSV
        csv_text = download_view_csv(server_url, token, site_id, view_id, vf_params)

        # 8) print preview + save
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
