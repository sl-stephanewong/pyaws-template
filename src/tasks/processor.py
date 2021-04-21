from abc import ABC, abstractmethod
import findspark
findspark.init()
from src.models.data_source import DataSource, DataFormat, JDBCDataSource
from pyspark.sql import DataFrame, DataFrameWriter
from src.utils.utils import AWSUtil


def format_save(writer: DataFrameWriter, data_source: DataSource) -> None:
    if data_source.data_format is DataFormat.CSV:
        writer.option("header", True).csv(data_source.source_path)
    if data_source.data_format is DataFormat.PARQUET:
        writer.parquet(data_source.source_path)
    if data_source.data_format is DataFormat.JSON:
        writer.json(data_source.source_path)
    if data_source.data_format is DataFormat.JDBC:
        if type(data_source) is JDBCDataSource:
            ## TODO
            writer.jdbc()


class Processor(ABC):

    @abstractmethod
    def run(self, df: DataFrame) -> None:
        pass

    def __init__(self, input_data_source: DataSource,
                 output_data_source: DataSource,
                 aws_util: AWSUtil = AWSUtil()) -> None:
        self._input_data_source = input_data_source
        self._output_data_source = output_data_source
        self._aws_util = aws_util


class WriterProcessor(Processor):

    def run(self, df: DataFrame) -> None:
        writer: DataFrameWriter = df.coalesce(self._output_data_source.partition_number)\
            .write.mode(self._output_data_source.mode)
        format_save(writer, self._output_data_source)

    def __init__(self,
                 input_data_source: DataSource,
                 output_data_source: DataSource,
                 aws_util: AWSUtil = AWSUtil()) -> None:
        super().__init__(input_data_source, output_data_source, aws_util)
