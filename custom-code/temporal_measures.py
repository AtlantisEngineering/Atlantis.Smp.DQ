"""Temporal data quality measures, loaded as custom code:

    streamdaq serve --files custom-code/

Delta measures take ``column`` and ``time_column`` (interarrival times: both set to the time column).
``*TimeFromNow`` measures take ``column`` and ``time_unit`` (unit of the timestamps; default ``"ms"``).
"""

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from statistics import mean, pvariance
from typing import ClassVar, Literal

import pathway as pw

from streamdaq.measures.any_column.tuple import Tuple
from streamdaq.measures.base import DataQualityMeasure
from streamdaq.measures.numeric.deltas_tuple import DeltasTuple
from streamdaq.utils.data_type_applicability import DataTypeApplicability


@dataclass
class VarianceDelta(DeltasTuple):
    """Population variance of the time-ordered deltas, consistent with ``Variance``."""

    _aggregator: ClassVar = lambda elements: float(pvariance(elements)) if len(elements) > 0 else 0.0
    _result_dtype: ClassVar[type] = float


@dataclass
class DeltaAboveCount(DeltasTuple):
    """Number of time-ordered deltas greater than ``threshold`` (or equal, if ``inclusive``)."""

    threshold: int | float
    inclusive: bool = field(default=False)
    _result_dtype: ClassVar[type] = int

    def _get_aggregator(self) -> Callable[[tuple], int]:
        threshold = self.threshold
        if self.inclusive:
            return lambda elements: sum(1 for element in elements if element >= threshold)
        return lambda elements: sum(1 for element in elements if element > threshold)


@dataclass
class DeltaBelowCount(DeltasTuple):
    """Number of time-ordered deltas less than ``threshold`` (or equal, if ``inclusive``)."""

    threshold: int | float
    inclusive: bool = field(default=False)
    _result_dtype: ClassVar[type] = int

    def _get_aggregator(self) -> Callable[[tuple], int]:
        threshold = self.threshold
        if self.inclusive:
            return lambda elements: sum(1 for element in elements if element <= threshold)
        return lambda elements: sum(1 for element in elements if element < threshold)


_UNITS_PER_SECOND: dict[str, int] = {"s": 1, "ms": 1_000, "us": 1_000_000, "ns": 1_000_000_000}


@dataclass
class TimeFromNow(DataQualityMeasure):
    """Base for measures over ``now - timestamp`` for every timestamp of ``column`` in the window.

    ``now`` is read when the window's result is computed, i.e. when the window closes, not when its
    data arrived. ``time_unit`` is the unit of the timestamps in ``column``; the result is in the
    same unit. An empty window returns 0.
    """

    time_unit: Literal["s", "ms", "us", "ns"] = field(default="ms")
    _applicability: ClassVar[DataTypeApplicability] = DataTypeApplicability.NUMERIC_ONLY
    _dependencies: ClassVar[list[type[DataQualityMeasure]]] = [Tuple]
    _should_be_registered: ClassVar[bool] = False
    _aggregator: ClassVar = None

    def __post_init__(self):
        if self.time_unit not in _UNITS_PER_SECOND:
            raise ValueError(
                f"Cannot initialize a {type(self).__name__} measure on column `{self.column}` "
                f"with time_unit '{self.time_unit}'. Valid options: {list(_UNITS_PER_SECOND)}."
            )

    def get_expression(self) -> pw.ColumnExpression:
        units_per_second = _UNITS_PER_SECOND[self.time_unit]
        aggregator = type(self)._aggregator

        def compute(timestamps: tuple) -> float:
            if len(timestamps) == 0:
                return 0.0
            now = time.time() * units_per_second  # read at computation time, not at build time
            return float(aggregator([now - timestamp for timestamp in timestamps]))

        return pw.apply_with_type(
            compute, float, pw.this[Tuple._get_internal_shared_column_name(self.column)]
        )


@dataclass
class MinTimeFromNow(TimeFromNow):
    """Age of the newest timestamp in the window (how stale the latest data is)."""

    _aggregator: ClassVar = min


@dataclass
class MaxTimeFromNow(TimeFromNow):
    """Age of the oldest timestamp in the window."""

    _aggregator: ClassVar = max


@dataclass
class MeanTimeFromNow(TimeFromNow):
    """Mean age of the timestamps in the window."""

    _aggregator: ClassVar = mean
