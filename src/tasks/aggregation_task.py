from abc import ABC
from ..utils.config import ConfigUtil
from .spark_session import SparkTask
from src.utils.utils import AWSUtil


class AggregationTask(SparkTask, ABC):

    def __init__(self,
                 config_filename: str = "pyaws.ini",
                 aws_util: AWSUtil = AWSUtil()) -> None:
        super().__init__(config_filename)