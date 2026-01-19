# export_dashboard_sheet_map.py
#
# Downloads a Tableau workbook's content (TWB/TWBX), parses dashboards and the worksheets used within them,
# and writes a CSV mapping: dashboard_name -> worksheet_name.
#
# Auth: Personal Access Token (PAT)
# Env vars required:
#   TABLEAU_SERVER_URL   e.g. https://us-east-1.online.tableau.com
#   TABLEAU_SITE         site contentUrl ("" for Default site)
#   TABLEAU_TOKEN_NAME
#   TABLEAU_TOKEN_VALUE
#
# Usage:
#   python export_dashboard_sheet_map.py --workbook "Book1" --out dashboard_sheet_map.csv
#
import argparse
import io
import os
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Set, Tuple

import requests
from dotenv import load_dotenv

API_VER = "3.19"
NS_REST = {"t": "http://tableau.com/api"}  # REST XML only (not TWB XML)


def must_env(k: str) -> str:
    v = os.getenv(k)
    if v is None:
        raise RuntimeError(f"Missing env var: {k}")
    return v


def tableau_headers(token: str) -> dict:
    return {"X-Tableau-Auth": token}


def signin_pat_xml(server_url: str, site_content_url: str, token_name: str, token_value: str) -> Tuple[str, str]:
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
        raise RuntimeError(f"Sign-in failed HTTP {r.status_code}\n{r.text[:1200]}")

    root = ET.fromstring(r.text)
    cred = root.find(".//t:credentials", NS_REST)
    site_el = root.find(".//t:site", NS_REST)
    if cred is None or "token" not in cred.attrib:
        raise RuntimeError("Could not find credentials token in sign-in response.")
    if site_el is None or "id" not in site_el.attrib:
        raise RuntimeError("Could not find site id in sign-in response.")

    return cred.attrib["token"], site_el.attrib["id"]


def signout(server_url: str, token: str) -> None:
    try:
        url = f"{server_url}/api/{API_VER}/auth/signout"
        requests.post(url, headers=tableau_headers(token), timeout=30)
    except Exception:
        pass


def find_workbook_id(server_url: str, token: str, site_id: str, workbook_name: str) -> str:
    url = f"{server_url}/api/{API_VER}/sites/{site_id}/workbooks"
    page = 1
    page_size = 100

    while True:
        r = requests.get(
            url,
            headers=tableau_headers(token),
            params={"pageNumber": page, "pageSize": page_size},
            timeout=60,
        )
        if r.status_code >= 400:
            raise RuntimeError(f"Workbooks list failed HTTP {r.status_code}\n{r.text[:1200]}")

        root = ET.fromstring(r.text)
        for wb in root.findall(".//t:workbook", NS_REST):
            if wb.attrib.get("name") == workbook_name:
                return wb.attrib["id"]

        pagination = root.find(".//t:pagination", NS_REST)
        if pagination is None:
            break
        total = int(pagination.attrib.get("totalAvailable", "0"))
        if page * page_size >= total:
            break
        page += 1

    raise RuntimeError(f"Workbook not found (exact name match): {workbook_name}")


def download_workbook_content(server_url: str, token: str, site_id: str, workbook_id: str) -> bytes:
    url = f"{server_url}/api/{API_VER}/sites/{site_id}/workbooks/{workbook_id}/content"
    r = requests.get(url, headers=tableau_headers(token), timeout=300)
    if r.status_code >= 400:
        raise RuntimeError(f"Workbook content download failed HTTP {r.status_code}\n{r.text[:1200]}")
    return r.content


def extract_twb_xml(content_bytes: bytes) -> bytes:
    # TWBX is ZIP; TWB is plain XML
    if content_bytes[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(content_bytes), "r") as z:
            twb_files = [n for n in z.namelist() if n.lower().endswith(".twb")]
            if not twb_files:
                raise RuntimeError("Downloaded TWBX but no .twb found inside.")
            with z.open(twb_files[0], "r") as f:
                return f.read()
    return content_bytes


def _collect_all_worksheet_names(root: ET.Element) -> Set[str]:
    """
    Collect worksheet names declared in the TWB.
    These are the canonical worksheet names we match dashboard zones against.
    """
    ws = set()
    for w in root.findall(".//worksheet"):
        n = (w.attrib.get("name") or "").strip()
        if n:
            ws.add(n)
    return ws


def parse_dashboard_to_worksheets(twb_xml_bytes: bytes) -> Dict[str, List[str]]:
    """
    Robust parsing strategy:
      - Get the full set of worksheet names declared in the workbook.
      - For each dashboard:
          - Look at every <zone ...> and:
              * if zone@name matches a worksheet name -> count it
              * also check nested <view name="..."> or similar nodes where present
    """
    root = ET.fromstring(twb_xml_bytes)

    worksheet_names = _collect_all_worksheet_names(root)
    dash_map: Dict[str, Set[str]] = {}

    for dash in root.findall(".//dashboard"):
        dash_name = (dash.attrib.get("name") or "").strip()
        if not dash_name:
            continue

        used: Set[str] = set()

        # 1) Primary: zone name matches worksheet name
        for zone in dash.findall(".//zone"):
            zn = (zone.attrib.get("name") or "").strip()
            if zn and zn in worksheet_names:
                used.add(zn)

            # 2) Fallback: sometimes worksheets are referenced via nested <view name="...">
            # (names often equal the worksheet name)
            for view in zone.findall(".//view"):
                vn = (view.attrib.get("name") or "").strip()
                if vn and vn in worksheet_names:
                    used.add(vn)

        dash_map[dash_name] = used

    return {k: sorted(v) for k, v in dash_map.items()}


def csv_escape(s: str) -> str:
    s = "" if s is None else str(s)
    if any(ch in s for ch in [",", '"', "\n", "\r"]):
        s = s.replace('"', '""')
        return f'"{s}"'
    return s


def write_csv(dash_to_sheets: Dict[str, List[str]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["dashboard_name,worksheet_name"]
    for dash in sorted(dash_to_sheets.keys()):
        sheets = dash_to_sheets[dash]
        if not sheets:
            lines.append(f"{csv_escape(dash)},")
        else:
            for sh in sheets:
                lines.append(f"{csv_escape(dash)},{csv_escape(sh)}")
    out_path.write_text("\n".join(lines), encoding="utf-8-sig")


def main():
    load_dotenv(override=True)

    ap = argparse.ArgumentParser()
    ap.add_argument("--workbook", required=True, help="Workbook DISPLAY name (exact match)")
    ap.add_argument("--out", default="dashboard_sheet_map.csv", help="Output CSV path")
    args = ap.parse_args()

    server_url = must_env("TABLEAU_SERVER_URL").rstrip("/")
    site = must_env("TABLEAU_SITE")  # contentUrl; "" for Default site
    token_name = must_env("TABLEAU_TOKEN_NAME")
    token_value = must_env("TABLEAU_TOKEN_VALUE")

    token = None
    try:
        token, site_id = signin_pat_xml(server_url, site, token_name, token_value)
        wb_id = find_workbook_id(server_url, token, site_id, args.workbook)

        content = download_workbook_content(server_url, token, site_id, wb_id)
        twb_xml = extract_twb_xml(content)

        dash_to_sheets = parse_dashboard_to_worksheets(twb_xml)
        write_csv(dash_to_sheets, Path(args.out))

        print(f"Workbook: {args.workbook} -> {wb_id}")
        print(f"Dashboards found: {len(dash_to_sheets)}")
        nonempty = sum(1 for v in dash_to_sheets.values() if v)
        print(f"Dashboards with >=1 worksheet match: {nonempty}")
        print(f"Saved CSV -> {Path(args.out).resolve()}")

    finally:
        if token:
            signout(server_url, token)


if __name__ == "__main__":
    main()
