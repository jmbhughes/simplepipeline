from __future__ import annotations

from datetime import datetime, timedelta
from typing import Sequence

from .models import (
    Product,
    Rule,
    latest_version_products,
    normalize_timestamp,
    validate_product_code,
)


class SameTimestampRule(Rule):
    """Select one input for each product code at the requested timestamp."""

    def __init__(
        self,
        name: str,
        input_product_code: str | Sequence[str],
        output_product_code: str,
    ) -> None:
        input_codes = (
            (input_product_code,)
            if isinstance(input_product_code, str)
            else tuple(input_product_code)
        )
        if not input_codes:
            raise ValueError("at least one input product code is required")
        for code in input_codes:
            validate_product_code(code)
        if len(set(input_codes)) != len(input_codes):
            raise ValueError("input product codes must be unique")
        super().__init__(name, input_codes[0], output_product_code)
        self._input_product_codes = input_codes

    @property
    def input_product_codes(self) -> tuple[str, ...]:
        return self._input_product_codes

    def select_inputs(self, candidates: Sequence[Product], output_timestamp: datetime) -> list[Product]:
        target = normalize_timestamp(output_timestamp)
        candidates = latest_version_products(candidates)
        selected = []
        for code in self.input_product_codes:
            matches = [
                product
                for product in candidates
                if product.product_code == code and product.timestamp == target
            ]
            if not matches:
                return []
            selected.append(
                min(matches, key=lambda p: (p.path, p.product_id or 0))
            )
        return selected


class EvenlyDistributedWindowRule(Rule):
    """Select exactly N inputs, as evenly distributed as possible in [T-window, T).

    Candidate inputs are sorted by timestamp. The implementation chooses indexes
    nearest to N evenly spaced target times at the centers of N equal-width bins.
    Any ties are resolved deterministically by timestamp, path, then database ID.
    """

    def __init__(
        self,
        name: str,
        input_product_code: str,
        output_product_code: str,
        *,
        window: timedelta,
        count: int,
    ) -> None:
        super().__init__(name, input_product_code, output_product_code)
        if window <= timedelta(0):
            raise ValueError("window must be positive")
        if count <= 0:
            raise ValueError("count must be positive")
        self.window = window
        self.count = count

    def select_inputs(self, candidates: Sequence[Product], output_timestamp: datetime) -> list[Product]:
        end = normalize_timestamp(output_timestamp)
        start = end - self.window
        eligible = sorted(
            (
                p for p in latest_version_products(candidates)
                if start <= p.timestamp < end
            ),
            key=lambda p: (p.timestamp, p.path, p.product_id or 0),
        )
        if len(eligible) < self.count:
            return []

        targets = [
            start + self.window * ((i + 0.5) / self.count)
            for i in range(self.count)
        ]
        selected: list[Product] = []
        used: set[int] = set()
        for target in targets:
            target_offset = (target - start).total_seconds()
            best_index = min(
                (i for i in range(len(eligible)) if i not in used),
                key=lambda i: (
                    abs((eligible[i].timestamp - start).total_seconds() - target_offset),
                    eligible[i].timestamp,
                    eligible[i].path,
                    eligible[i].product_id or 0,
                ),
            )
            selected.append(eligible[best_index])
            used.add(best_index)
        return sorted(selected, key=lambda p: (p.timestamp, p.path, p.product_id or 0))
