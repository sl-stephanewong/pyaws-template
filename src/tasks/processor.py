from abc import ABC, abstractmethod
import findspark
findspark.init()
from .data_source import DataSource, DataFormat
from pyspark.sql import DataFrame, DataFrameWriter


def format_save(writer: DataFrameWriter, path: str, data_format: str = DataFormat.PARQUET) -> None:
    if data_format is DataFormat.CSV:
        writer.csv(path)
    if data_format is DataFormat.PARQUET:
        writer.parquet(path)
    if data_format is DataFormat.JSON:
        writer.json(path)


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
        format_save(writer, self.output_data_source.source_path, self.output_data_source.data_format)

    def __init__(self,
                 input_data_source: DataSource,
                 output_data_source: DataSource) -> None:
        super().__init__(input_data_source, output_data_source)
