import logging
import findspark
findspark.init()
from .processor import Processor, WriterProcessor
from .data_source import DataFormat, DataSource
from abc import ABC, abstractmethod


from pyspark.sql import *


class Session:

    def init_session(self, app_name: str) -> SparkSession:
        self.spark_session = SparkSession \
            .builder \
            .master("local[*]") \
            .appName(app_name) \
            .getOrCreate()

    def read(self, data_source: DataSource) -> DataFrame:
        source_path = data_source.source_path
        reader: DataFrameReader = self.spark_session.read
        if data_source.data_format == DataFormat.CSV:
            has_header: bool = data_source.options.get("header")
            print(source_path)
            if has_header is None:
                has_header = False
            return reader.csv(path=source_path, header=True)
        if data_source.data_format == DataFormat.JSON:
            return reader.json(source_path)
        if data_source.data_format == DataFormat.PARQUET:
            return reader.parquet(path=source_path, options=data_source.options)

    def __init__(self, app_name: str = "default session name"):
        self.spark_session = None
        self.init_session(app_name)


class SparkTask(ABC):

    @abstractmethod
    def _aggregation(self, df: DataFrame) -> DataFrame:
        pass

    def _before_save(self) -> None:
        pass

    def do_save(self) -> DataFrame:
        self._before_save()
        # Call aggregation from specific task
        aggregation = self._aggregation(self.session.read(self.input_data_source))
        # Call run on every processor
        for processor in self.processors:
            processor.run(aggregation)
        self._after_save(aggregation)

    def _after_save(self, df: DataFrame) -> None:
        pass

    def __init__(self, input_data_source: DataSource,
                 output_data_source: DataSource,
                 task_name: str = "default task name") -> None:
        logging.info("Init spark task: ", task_name)
        self.input_data_source = input_data_source
        self.output_data_source = output_data_source
        self.processors: list[Processor] = [WriterProcessor(input_data_source, output_data_source)]
        self.session = Session(task_name)
