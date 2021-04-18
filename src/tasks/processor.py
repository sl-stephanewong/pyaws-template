from abc import ABC, abstractmethod
import findspark
findspark.init()
from src.models.data_source import DataSource, DataFormat, JDBCDataSource
from pyspark.sql import DataFrame, DataFrameWriter


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
                 output_data_source: DataSource) -> None:
        self.input_data_source = input_data_source
        self.output_data_source = output_data_source


class WriterProcessor(Processor):

    def run(self, df: DataFrame) -> None:
        writer: DataFrameWriter = df.coalesce(self.output_data_source.partition_number)\
            .write.mode(self.output_data_source.mode)
        format_save(writer, self.output_data_source)

    def __init__(self,
                 input_data_source: DataSource,
                 output_data_source: DataSource) -> None:
        super().__init__(input_data_source, output_data_source)
