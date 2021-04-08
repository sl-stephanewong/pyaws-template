from abc import ABC
from ..utils.config import ConfigUtil
from .spark_session import SparkTask


class AggregationTask(SparkTask, ABC):

    def __init__(self, config_filename: str = "pyaws.ini") -> None:
        super().__init__(config_filename)