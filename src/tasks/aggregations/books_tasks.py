import findspark

findspark.init()
from pyspark.sql import DataFrame
from src.models.data_source import FileDataSource, DataFormat, DataSource, SourceKey
from ..aggregation_task import AggregationTask
from ..spark_session import Session


class BookTask(AggregationTask):

    def __init__(self,
                 config_filename: str = "pyaws.ini",
                 global_section_name: str = "tasks.books_task") -> None:
        super(BookTask, self).__init__(config_filename)
        task_name = self._config.get_option(global_section_name, "task_name")
        input_section = global_section_name + ".books"
        output_section = global_section_name + ".output_data_source"
        self._input_data_source: DataSource = FileDataSource(
            source_path=self._config.get_option(input_section, SourceKey.SRC_PATH.value),
            data_format=DataFormat[self._config.get_option(input_section, SourceKey.DATA_FORMAT.value)]
        )
        self._output_data_source: DataSource = FileDataSource(
            source_path=self._config.get_option(output_section, SourceKey.SRC_PATH.value),
            data_format=DataFormat[self._config.get_option(output_section, SourceKey.DATA_FORMAT.value)]
        )
        self._session = Session(task_name)

    def _aggregation(self, df_source: DataFrame) -> DataFrame:

        self._session.spark_session._jsc.hadoopConfiguration()\
            .set("fs.s3a.endpoint", "s3.eu-west-3.amazonaws.com")
        self._session.spark_session._jsc.hadoopConfiguration()\
            .set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        self._session.spark_session._jsc.hadoopConfiguration()\
            .set("fs.s3a.access.key", self._aws_util.get_credentials().get_frozen_credentials().access_key)
        self._session.spark_session._jsc.hadoopConfiguration()\
            .set("fs.s3a.secret.key", self._aws_util.get_credentials().get_frozen_credentials().secret_key)
        return df_source
