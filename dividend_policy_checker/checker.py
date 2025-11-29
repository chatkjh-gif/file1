"""Domain logic for evaluating dividend policy adoption among KOSPI 200 firms."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Sequence

from .dart import DartClient


DEFAULT_KEYWORDS: Sequence[str] = (
    "배당기준일",
    "배당 기준일",
    "배당액 공시",
    "정관변경",
    "정관 변경",
    "배당 관련 정관",
)


@dataclass
class CompanyResult:
    """Evaluation result for a single corporation."""

    corp_code: str
    corp_name: str
    stock_code: str
    has_post_dividend_provision: bool
    matching_reports: List[Dict[str, str]] = field(default_factory=list)


class DividendPolicyChecker:
    """Analyze disclosures to infer post-dividend-date designation adoption."""

    def __init__(
        self,
        dart_client: DartClient,
        *,
        start_date: str,
        end_date: str,
        keywords: Sequence[str] = DEFAULT_KEYWORDS,
    ) -> None:
        self.dart = dart_client
        self.start_date = start_date
        self.end_date = end_date
        self.keywords = tuple(keywords)

    def _matches_policy(self, report_name: str) -> bool:
        return any(keyword in report_name for keyword in self.keywords)

    def evaluate_company(self, corp: Dict[str, str]) -> CompanyResult:
        corp_code = corp.get("corp_code") or ""
        corp_name = corp.get("corp_name") or corp.get("name") or ""
        stock_code = corp.get("stock_code") or ""

        filings = self.dart.search_filings(
            corp_code,
            start_date=self.start_date,
            end_date=self.end_date,
            detail_types="B001,I001",
        )

        matches = [item for item in filings if self._matches_policy(item.get("report_nm", ""))]
        return CompanyResult(
            corp_code=corp_code,
            corp_name=corp_name,
            stock_code=stock_code,
            has_post_dividend_provision=bool(matches),
            matching_reports=matches,
        )

    def evaluate_all(self, corporations: Iterable[Dict[str, str]]) -> List[CompanyResult]:
        results: List[CompanyResult] = []
        for corp in corporations:
            results.append(self.evaluate_company(corp))
        return results
