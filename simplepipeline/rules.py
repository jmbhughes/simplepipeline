from __future__ import annotations

import random
from datetime import datetime, timedelta
from typing import Callable, Sequence, cast

from .models import (
    Product,
    Rule,
    ScheduledRule,
    latest_version_products,
    normalize_timestamp,
    semantic_version_key,
    validate_product_code,
)


class LatestBeforeRule(Rule):
    """Select the highest-version input before the output time."""

    def __init__(
        self, name: str, input_product_code: str, output_product_code: str
    ) -> None:
        super().__init__(name, input_product_code, output_product_code)

    def select_inputs(
        self, candidates: Sequence[Product], output_timestamp: datetime
    ) -> list[Product]:
        target = normalize_timestamp(output_timestamp)
        eligible = [
            product
            for product in candidates
            if product.product_code == self.input_product_code
            and product.timestamp < target
        ]
        if not eligible:
            return []
        highest_version = max(
            (product.version for product in eligible),
            key=semantic_version_key,
        )
        latest_timestamp = max(
            product.timestamp
            for product in eligible
            if product.version == highest_version
        )
        return [
            min(
                (
                    product
                    for product in eligible
                    if product.version == highest_version
                    and product.timestamp == latest_timestamp
                ),
                key=lambda product: (product.path, product.product_id or 0),
            )
        ]

    def pending_reasons(
        self, candidates: Sequence[Product], output_timestamp: datetime
    ) -> list[str]:
        if self.select_inputs(candidates, output_timestamp):
            return []
        return [
            f"{self.name}: waiting for {self.input_product_code} before "
            f"{normalize_timestamp(output_timestamp).isoformat()}"
        ]


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

    def pending_reasons(
        self, candidates: Sequence[Product], output_timestamp: datetime
    ) -> list[str]:
        target = normalize_timestamp(output_timestamp)
        available = latest_version_products(candidates)
        missing = [
            code
            for code in self.input_product_codes
            if not any(
                product.product_code == code and product.timestamp == target
                for product in available
            )
        ]
        if not missing:
            return []
        return [
            f"{self.name}: waiting for {', '.join(missing)} at {target.isoformat()}"
        ]


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
                if p.product_code == self.input_product_code
                and start <= p.timestamp < end
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

    def pending_reasons(
        self, candidates: Sequence[Product], output_timestamp: datetime
    ) -> list[str]:
        end = normalize_timestamp(output_timestamp)
        start = end - self.window
        eligible = [
            product
            for product in latest_version_products(candidates)
            if product.product_code == self.input_product_code
            and start <= product.timestamp < end
        ]
        if len(eligible) >= self.count:
            return []
        return [
            f"{self.name}: waiting for {self.count - len(eligible)} more "
            f"{self.input_product_code} product(s) in [{start.isoformat()}, "
            f"{end.isoformat()}) ({len(eligible)}/{self.count} available)"
        ]


class RandomDistributedWindowRule(Rule):
    """Select a random sample of exactly N inputs from [T-window, T)."""

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

    def select_inputs(
        self, candidates: Sequence[Product], output_timestamp: datetime
    ) -> list[Product]:
        end = normalize_timestamp(output_timestamp)
        start = end - self.window
        eligible = sorted(
            (
                product
                for product in latest_version_products(candidates)
                if product.product_code == self.input_product_code
                and start <= product.timestamp < end
            ),
            key=lambda product: (
                product.timestamp,
                product.path,
                product.product_id or 0,
            ),
        )
        if len(eligible) < self.count:
            return []
        return sorted(
            random.sample(eligible, self.count),
            key=lambda product: (
                product.timestamp,
                product.path,
                product.product_id or 0,
            ),
        )

    def pending_reasons(
        self, candidates: Sequence[Product], output_timestamp: datetime
    ) -> list[str]:
        end = normalize_timestamp(output_timestamp)
        start = end - self.window
        eligible = [
            product
            for product in latest_version_products(candidates)
            if product.product_code == self.input_product_code
            and start <= product.timestamp < end
        ]
        if len(eligible) >= self.count:
            return []
        return [
            f"{self.name}: waiting for {self.count - len(eligible)} more "
            f"{self.input_product_code} product(s) in [{start.isoformat()}, "
            f"{end.isoformat()}) ({len(eligible)}/{self.count} available)"
        ]


class CompositeRule:
    """Require the selected inputs from each child rule."""

    def __init__(self, name: str, rules: Sequence[ScheduledRule]) -> None:
        if not name or not name.strip():
            raise ValueError("rule name cannot be empty")
        if not rules:
            raise ValueError("at least one rule is required")
        output_codes = {rule.output_product_code for rule in rules}
        if len(output_codes) != 1:
            raise ValueError("all rules must produce the same output product code")

        self.name = name
        self.rules = tuple(rules)
        self.output_product_code = rules[0].output_product_code
        self._input_product_codes = tuple(
            dict.fromkeys(
                code for rule in rules for code in rule.input_product_codes
            )
        )

    @property
    def input_product_code(self) -> str:
        return self._input_product_codes[0]

    @property
    def input_product_codes(self) -> tuple[str, ...]:
        return self._input_product_codes

    def select_inputs(
        self, candidates: Sequence[Product], output_timestamp: datetime
    ) -> list[Product]:
        selected: list[Product] = []
        seen: set[tuple[str, ...]] = set()
        for rule in self.rules:
            rule_inputs = rule.select_inputs(candidates, output_timestamp)
            if not rule_inputs:
                return []
            for product in rule_inputs:
                identity = (
                    ("id", str(product.product_id))
                    if product.product_id is not None
                    else (
                        "product",
                        product.product_code,
                        product.timestamp.isoformat(),
                        product.path,
                        product.version,
                    )
                )
                if identity not in seen:
                    selected.append(product)
                    seen.add(identity)
        return selected

    def pending_reasons(
        self, candidates: Sequence[Product], output_timestamp: datetime
    ) -> list[str]:
        reasons = []
        for rule in self.rules:
            if rule.select_inputs(candidates, output_timestamp):
                continue
            describe = getattr(rule, "pending_reasons", None)
            if callable(describe):
                reasons.extend(
                    cast(
                        Callable[[Sequence[Product], datetime], list[str]],
                        describe,
                    )(candidates, output_timestamp)
                )
            else:
                reasons.append(f"{rule.name}: required inputs are not available")
        return reasons
