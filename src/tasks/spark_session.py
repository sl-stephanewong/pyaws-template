import logging
import findspark
from enum import Enum
from abc import ABC, abstractmethod

findspark.init()

from pyspark.sql import *


class DataFormat(Enum):
    CSV = "csv"
    PARQUET = "parquet"
    JSON = "json"


class DataSource:

    # default format is parquet
    def __init__(self,
                 source_path: str,
                 data_format: str = DataFormat.PARQUET,
                 partition_number: int = 8,
                 partition_by: str = None,
                 mode: str = "append",
                 **options: dict) -> None:
        self.source_path = source_path
        self.data_format = data_format
        self.partition_number = partition_number
        self.partition_by = partition_by
        self.mode = mode
        self.options = options


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
            if has_header is None:
                has_header = False
            return reader.csv(path=source_path, header=has_header)
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
        aggregation = self._aggregation(
            self.session.read(
                self.input_data_source.source_path,
                self.input_data_source.data_format
            )
        )
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


class SpecificTask(SparkTask):

    def _aggregation(self, df: DataFrame) -> DataFrame:
        df.show()
        d = df.filter(df['age'] > 35)
        d.show()
        return d

    def __init__(self, input_data_source: DataSource, output_data_source: DataSource,
                 task_name: str = "default task name") -> None:
        super().__init__(input_data_source, output_data_source, task_name)


class Processor(ABC):

    @abstractmethod
    def run(self, df: DataFrame) -> None:
        pass

    def __init__(self, input_data_source: DataSource,
                 output_data_source: DataSource) -> None:
        self.input_data_source = input_data_source
        self.output_data_source = output_data_source


class WriterProcessor(Processor):

    def run(self, df: DataFrame) -> None:
        df.coalesce(self.output_data_source.partition_number).write \
            .save(path=self.output_data_source.source_path,
                  format=self.output_data_source.data_format,
                  partitionBy=self.output_data_source.partition_by,
                  mode=self.output_data_source.mode,
                  options=self.output_data_source.options)

    def __init__(self,
                 input_data_source: DataSource,
                 output_data_source: DataSource) -> None:
        super().__init__(input_data_source, output_data_source)
