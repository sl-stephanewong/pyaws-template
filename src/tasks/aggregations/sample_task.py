import findspark

findspark.init()
from ..data_source import DataFormat, FileDataSource, DataSource, SourceKey
from pyspark.sql import DataFrame
from ..aggregation_task import AggregationTask
from ..spark_session import Session


class SampleTask(AggregationTask):

    def __init__(self,
                 config_filename: str = "pyaws.ini",
                 global_section_name: str = "tasks.sample_task") -> None:
        super(SampleTask, self).__init__(config_filename)
        task_name = self._config.get_option(global_section_name, "task_name")
        input_section = global_section_name + ".input_data_source"
        output_section = global_section_name + ".output_data_source"
        self.input_data_source: DataSource = FileDataSource(
            source_path=self._config.get_option(input_section, SourceKey.SRC_PATH.value),
            data_format=DataFormat[self._config.get_option(input_section, SourceKey.DATA_FORMAT.value)]
        )
        self.output_data_source: DataSource = FileDataSource(
            source_path=self._config.get_option(output_section, SourceKey.SRC_PATH.value),
            data_format=DataFormat[self._config.get_option(output_section, SourceKey.DATA_FORMAT.value)]
        )
        self.session = Session(task_name)

    def aggregation(self, df: DataFrame) -> DataFrame:
        df.show()
        old = df.filter(df["age"] > 35)
        old.show()
        return old
