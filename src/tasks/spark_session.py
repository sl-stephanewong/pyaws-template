import logging
import findspark

findspark.init()
from .processor import Processor, WriterProcessor
from src.models.data_source import DataSource
from abc import ABC, abstractmethod
from ..utils.config import ConfigUtil
from src.utils.utils import *

from pyspark.sql import *


class Session:

    def init_session(self, app_name: str = "default session name") -> SparkSession:
        logging.info("Init spark task name: ", app_name)
        self._spark_session: SparkSession = SparkSession \
            .builder \
            .master("local[*]") \
            .appName(app_name) \
            .enableHiveSupport() \
            .getOrCreate()

    def read(self, data_source: DataSource) -> DataFrame:
        # TODO read jdbc
        source_path = data_source.source_path
        reader: DataFrameReader = self._spark_session.read
        return reader.option("header", True) \
            .load(path=source_path, format=data_source.data_format.value, options=data_source.options)

    def __init__(self, app_name: str = "default session name"):
        self._spark_session: SparkSession = None
        self.init_session(app_name)

    @property
    def spark_session(self):
        return self._spark_session


class SparkTask(ABC):

    @abstractmethod
    def _aggregation(self, df_source: DataFrame = None) -> DataFrame:
        pass

    def _before_save(self, df_source: DataFrame) -> None:
        pass

    def _do_save(self, df: DataFrame) -> DataFrame:
        # Call aggregation from specific task
        aggregation = self._aggregation(df)
        # Call run on every processor
        for processor in self._processors:
            processor.run(aggregation)
        return aggregation

    def _after_save(self, aggregation: DataFrame) -> None:
        pass

    def run(self) -> None:
        self._processors: list[Processor] = [WriterProcessor(self._input_data_source, self._output_data_source)]
        df_source = self._session.read(self._input_data_source)
        self._before_save(df_source)
        aggregation = self._do_save(df_source)
        self._after_save(aggregation)

    def __init__(self, config_filename: str = "pyaws.ini") -> None:
        self._config = ConfigUtil(config_filename)
        self._aws_util: AWSUtil = AWSUtil()
        self._input_data_source: DataSource = None
        self._output_data_source: DataSource = None
        self._processors: list[Processor] = None
        self._session: Session = None
