"""CLI to check whether KOSPI 200 firms adopted post-dividend-date designations."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, timedelta
from typing import List

from dividend_policy_checker.checker import DividendPolicyChecker
from dividend_policy_checker.dart import DartClient, DartError
from dividend_policy_checker.krx import fetch_kospi200, normalize_constituents, KrxError


def _default_start_date() -> str:
    # Inspect the last three years of disclosures by default.
    today = date.today()
    start = today - timedelta(days=365 * 3)
    return start.strftime("%Y%m%d")


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", default=_default_start_date(), help="검색 시작일 (YYYYMMDD)")
    parser.add_argument("--end-date", default=date.today().strftime("%Y%m%d"), help="검색 종료일 (YYYYMMDD)")
    parser.add_argument("--output", help="결과를 JSON 파일로 저장")
    parser.add_argument(
        "--dart-api-key",
        default=os.getenv("DART_API_KEY"),
        help="DART Open API 인증키 (미지정 시 DART_API_KEY 환경 변수 사용)",
    )
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv or [])

    api_key = args.dart_api_key
    if not api_key:
        print("환경 변수 DART_API_KEY 또는 --dart-api-key 값이 필요합니다.", file=sys.stderr)
        return 1

    dart = DartClient(api_key)
    try:
        corp_codes = dart.download_corp_codes()
    except Exception as exc:  # noqa: BLE001 - surface network issues to users
        print(f"DART 법인코드 다운로드 실패: {exc}", file=sys.stderr)
        return 1

    corp_index = dart.build_corp_index(corp_codes)

    try:
        raw_constituents = fetch_kospi200()
    except KrxError as exc:
        print(f"KRX 조회 실패: {exc}", file=sys.stderr)
        return 1
    constituents = normalize_constituents(raw_constituents)

    # Join KRX constituents to DART corp codes using stock codes.
    enriched = []
    for item in constituents:
        match = corp_index.get(item["stock_code"])
        if not match:
            continue
        enriched.append({
            **item,
            "corp_code": match["corp_code"],
            "corp_name": match["corp_name"] or item.get("name", ""),
        })

    checker = DividendPolicyChecker(
        dart,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    try:
        results = checker.evaluate_all(enriched)
    except DartError as exc:
        print(f"공시 조회 실패: {exc}", file=sys.stderr)
        return 1

    summary = [
        {
            "corp_code": r.corp_code,
            "corp_name": r.corp_name,
            "stock_code": r.stock_code,
            "has_post_dividend_provision": r.has_post_dividend_provision,
            "matching_reports": [report.get("report_nm") for report in r.matching_reports],
        }
        for r in results
    ]

    if args.output:
        with open(args.output, "w", encoding="utf-8") as fp:
            json.dump(summary, fp, ensure_ascii=False, indent=2)
    else:
        print(json.dumps(summary, ensure_ascii=False, indent=2))

    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
