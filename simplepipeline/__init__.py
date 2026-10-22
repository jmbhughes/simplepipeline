"""Generic, SQLite-backed scientific data pipeline primitives."""

from .engine import DailyWorker, Pipeline
from .config import ConfigWorker
from .models import Product, Rule
from .rules import (
    EvenlyDistributedWindowRule,
    LatestBeforeRule,
    RandomDistributedWindowRule,
    SameTimestampRule,
)

__all__ = [
    "EvenlyDistributedWindowRule",
    "LatestBeforeRule",
    "RandomDistributedWindowRule",
    "ConfigWorker",
    "DailyWorker",
    "Pipeline",
    "Product",
    "Rule",
    "SameTimestampRule",
]
