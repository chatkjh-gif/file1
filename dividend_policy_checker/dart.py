"""Client helpers for the DART open API."""
from __future__ import annotations

import io
import json
import zipfile
from typing import Dict, Iterable, List, Optional
from urllib import parse, request
import xml.etree.ElementTree as ET

CORP_CODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
LIST_URL = "https://opendart.fss.or.kr/api/list.json"


class DartError(RuntimeError):
    """Raised when the DART service responds with an error."""


class DartClient:
    """Small wrapper around the DART API endpoints used in this project."""

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("DART API key is required")
        self.api_key = api_key

    def _get_json(self, url: str, params: Dict[str, str]) -> Dict:
        params = {**params, "crtfc_key": self.api_key}
        encoded = parse.urlencode(params)
        target = f"{url}?{encoded}"
        req = request.Request(target)
        req.add_header("User-Agent", "Mozilla/5.0")
        with request.urlopen(req, timeout=20) as resp:
            payload = resp.read().decode("utf-8")
        data = json.loads(payload)
        status = data.get("status")
        if status not in ("000", None):
            message = data.get("message") or "Unexpected response"
            raise DartError(f"DART error {status}: {message}")
        return data

    def download_corp_codes(self) -> List[Dict[str, str]]:
        """Download and parse the master corporation code file."""

        encoded = parse.urlencode({"crtfc_key": self.api_key})
        target = f"{CORP_CODE_URL}?{encoded}"
        req = request.Request(target)
        req.add_header("User-Agent", "Mozilla/5.0")
        with request.urlopen(req, timeout=30) as resp:
            archive = resp.read()

        with zipfile.ZipFile(io.BytesIO(archive)) as zf:
            xml_bytes = zf.read("CORPCODE.xml")

        tree = ET.fromstring(xml_bytes)
        result: List[Dict[str, str]] = []
        for element in tree.iter("list"):
            result.append({
                "corp_code": element.findtext("corp_code", default="").strip(),
                "corp_name": element.findtext("corp_name", default="").strip(),
                "stock_code": element.findtext("stock_code", default="").strip(),
                "modify_date": element.findtext("modify_date", default="").strip(),
            })
        return result

    @staticmethod
    def build_corp_index(codes: Iterable[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
        """Return a mapping of stock_code to corp metadata."""

        index: Dict[str, Dict[str, str]] = {}
        for entry in codes:
            stock_code = (entry.get("stock_code") or "").strip()
            corp_code = (entry.get("corp_code") or "").strip()
            if not stock_code or not corp_code:
                continue
            index[stock_code] = entry
        return index

    def search_filings(
        self,
        corp_code: str,
        *,
        start_date: str,
        end_date: str,
        detail_types: Optional[str] = None,
        page_count: int = 100,
    ) -> List[Dict[str, str]]:
        """Search filings for a single corporation between two dates.

        ``detail_types`` accepts comma-separated detail codes such as ``B001``
        (articles of association) and ``I001`` (board resolutions).
        """

        if not corp_code:
            raise ValueError("corp_code is required")

        page = 1
        filings: List[Dict[str, str]] = []
        while True:
            params = {
                "corp_code": corp_code,
                "bgn_de": start_date,
                "end_de": end_date,
                "page_no": page,
                "page_count": page_count,
            }
            if detail_types:
                params["pblntf_detail_ty"] = detail_types

            data = self._get_json(LIST_URL, params)
            items = data.get("list") or []
            filings.extend(items)

            total_pages = int(data.get("total_page") or 1)
            if page >= total_pages:
                break
            page += 1
        return filings
