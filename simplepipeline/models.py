from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol, Sequence


def normalize_timestamp(value: datetime) -> datetime:
    """Require timezone-aware timestamps and normalize them to UTC."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def timestamp_text(value: datetime) -> str:
    return normalize_timestamp(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return normalize_timestamp(parsed)


def validate_product_code(code: str) -> str:
    if not isinstance(code, str) or not code or len(code) > 16:
        raise ValueError("product code must be a non-empty string of at most 16 characters")
    if code.strip() != code:
        raise ValueError("product code cannot have leading or trailing whitespace")
    return code


_SEMVER_PATTERN = re.compile(
    r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|[0-9A-Za-z-]*[A-Za-z-][0-9A-Za-z-]*)"
    r"(?:\.(?:0|[1-9]\d*|[0-9A-Za-z-]*[A-Za-z-][0-9A-Za-z-]*))*))?"
    r"(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
)


def semantic_version_key(version: str) -> tuple:
    """Return a SemVer precedence key with a deterministic build-metadata tie-break."""
    if not isinstance(version, str):
        raise ValueError(f"invalid semantic version: {version!r}")
    match = _SEMVER_PATTERN.fullmatch(version)
    if match is None:
        raise ValueError(f"invalid semantic version: {version!r}")
    major, minor, patch, prerelease, build = match.groups()
    prerelease_key = ()
    if prerelease is not None:
        prerelease_key = tuple(
            (0, int(identifier))
            if identifier.isdigit()
            else (1, identifier)
            for identifier in prerelease.split(".")
        )
    return (
        int(major),
        int(minor),
        int(patch),
        prerelease is None,
        prerelease_key,
        build or "",
    )


def latest_version_products(products: Sequence[Product]) -> list[Product]:
    latest_versions: dict[str, str] = {}
    for product in products:
        current = latest_versions.get(product.product_code)
        if current is None or semantic_version_key(product.version) > semantic_version_key(current):
            latest_versions[product.product_code] = product.version
    return [
        product
        for product in products
        if product.version == latest_versions[product.product_code]
    ]


@dataclass(frozen=True)
class Product:
    product_code: str
    timestamp: datetime
    path: str
    product_id: int | None = None
    version: str = "0.0.0"

    def __post_init__(self) -> None:
        validate_product_code(self.product_code)
        if not self.path:
            raise ValueError("product path cannot be empty")
        object.__setattr__(self, "timestamp", normalize_timestamp(self.timestamp))
        semantic_version_key(self.version)


@dataclass(frozen=True)
class Rule:
    """A rule maps an output timestamp to an ordered set of input products."""

    name: str
    input_product_code: str
    output_product_code: str

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise ValueError("rule name cannot be empty")
        validate_product_code(self.input_product_code)
        validate_product_code(self.output_product_code)

    @property
    def input_product_codes(self) -> tuple[str, ...]:
        return (self.input_product_code,)

    def select_inputs(self, candidates: Sequence[Product], output_timestamp: datetime) -> list[Product]:
        raise NotImplementedError

    def pending_reasons(
        self, candidates: Sequence[Product], output_timestamp: datetime
    ) -> list[str]:
        return [f"{self.name}: required inputs are not available"]


class ScheduledRule(Protocol):
    name: str
    input_product_code: str
    output_product_code: str
    input_product_codes: tuple[str, ...]

    def select_inputs(self, candidates: Sequence[Product], output_timestamp: datetime) -> list[Product]: ...
