import logging
import findspark

findspark.init()
from .processor import Processor, WriterProcessor
from .data_source import DataFormat, DataSource
from abc import ABC, abstractmethod
from ..utils.config import ConfigUtil

from pyspark.sql import *


class Session:

    def init_session(self, app_name: str) -> SparkSession:
        logging.info("Init spark task name: ", app_name)
        self.spark_session: SparkSession = SparkSession \
            .builder \
            .master("local[*]") \
            .appName(app_name) \
            .enableHiveSupport() \
            .getOrCreate()

    def read(self, data_source: DataSource) -> DataFrame:
        source_path = data_source.source_path
        reader: DataFrameReader = self.spark_session.read
        return reader.option("header", True) \
            .load(path=source_path, format=data_source.data_format.value, options=data_source.options)

    def __init__(self, app_name: str = "default session name"):
        self.spark_session: SparkSession = None
        self.init_session(app_name)


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
        for processor in self.processors:
            processor.run(aggregation)
        return aggregation

    def _after_save(self, aggregation: DataFrame) -> None:
        pass

    def run(self) -> None:
        self.processors: list[Processor] = [WriterProcessor(self.input_data_source, self.output_data_source)]
        df_source = self.session.read(self.input_data_source)
        self._before_save(df_source)
        aggregation = self._do_save(df_source)
        self._after_save(aggregation)

    def __init__(self, config_filename: str = "pyaws.ini") -> None:
        self._config = ConfigUtil(config_filename)
        self.input_data_source: DataSource = None
        self.output_data_source: DataSource = None
        self.processors: list[Processor] = None
        self.session: Session = None
