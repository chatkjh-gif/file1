"""Minimal client for downloading KOSPI 200 constituents from KRX."""
from __future__ import annotations

import datetime as _dt
import json
from typing import Dict, Iterable, List
from urllib import parse, request


KRX_ENDPOINT = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
_KRX_BLD = "dbms/MDC/STAT/standard/MDCSTAT00601"


class KrxError(RuntimeError):
    """Raised when the KRX service returns an unexpected response."""


def _post_json(payload: Dict[str, str], timeout: int = 15) -> Dict:
    encoded = parse.urlencode(payload).encode("utf-8")
    req = request.Request(KRX_ENDPOINT, data=encoded, method="POST")
    req.add_header("User-Agent", "Mozilla/5.0")
    with request.urlopen(req, timeout=timeout) as resp:
        content = resp.read().decode("utf-8")
    return json.loads(content)


def fetch_kospi200(trade_date: str | None = None) -> List[Dict[str, str]]:
    """Return raw KOSPI 200 constituent data.

    The KRX endpoint expects a trade date in ``YYYYMMDD`` format. If omitted,
    the most recent date is used.
    """

    if trade_date is None:
        trade_date = _dt.date.today().strftime("%Y%m%d")

    payload = {
        "bld": _KRX_BLD,
        # ``trdDd`` picks the evaluation date. The API falls back to the
        # latest trading session when provided with a future date.
        "trdDd": trade_date,
        # ``idxIndCd`` identifies the KOSPI 200 index within the endpoint.
        "idxIndCd": "1",
    }
    data = _post_json(payload)

    if "OutBlock_1" not in data:
        raise KrxError(f"Unexpected KRX response keys: {sorted(data)}")

    return list(data.get("OutBlock_1") or [])


def normalize_constituents(entries: Iterable[Dict[str, str]]) -> List[Dict[str, str]]:
    """Normalize raw KRX entries into a consistent shape.

    The service occasionally changes field names; this helper coalesces the
    common ones into a stable interface.
    """

    normalized: List[Dict[str, str]] = []
    for entry in entries:
        stock_code = (
            entry.get("ISU_SRT_CD")
            or entry.get("TDD_CLSPRC")  # Alternate ticker field in some payloads
            or entry.get("CMP_CD")
        )
        name = entry.get("ISU_ABBRV") or entry.get("CMP_KOR") or entry.get("KOR_SHRT_NM")
        if not stock_code or not name:
            # Skip malformed rows while keeping track of the original payload for
            # potential debugging.
            continue
        normalized.append({
            "stock_code": stock_code.strip(),
            "name": name.strip(),
            "market": entry.get("MKT_ID") or entry.get("MKT_NM") or "KOSPI",
        })
    return normalized
