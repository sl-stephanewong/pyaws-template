import findspark

findspark.init()
from pyspark.sql import DataFrame
from src.utils.utils import *
from ..aggregation_task import AggregationTask
from ..spark_session import Session


class BookRecommenderTask(AggregationTask):

    def __init__(self,
                 config_filename: str = "pyaws.ini",
                 global_section_name: str = "tasks.books_task") -> None:
        super(BookRecommenderTask, self).__init__(config_filename)
        task_name = self._config.get_option(global_section_name, "task_name")
        input_section = global_section_name + ".books"
        output_section = global_section_name + ".output_data_source"
        self.input_data_source: DataSource = FileDataSource(
            source_path=self._config.get_option(input_section, SourceKey.SRC_PATH.value),
            data_format=DataFormat[self._config.get_option(input_section, SourceKey.DATA_FORMAT.value)]
        )
        self.output_data_source: DataSource = FileDataSource(
            source_path=self._config.get_option(output_section, SourceKey.SRC_PATH.value),
            data_format=DataFormat[self._config.get_option(output_section, SourceKey.DATA_FORMAT.value)]
        )
        self.ratings_data_source = get_file_data_source(
            self._config.get_option(global_section_name + ".ratings", SourceKey.SRC_PATH.value))
        self.users_data_source = get_file_data_source(
            self._config.get_option(global_section_name + ".users", SourceKey.SRC_PATH.value))
        self.session = Session(task_name)

    def _aggregation(self, df_source: DataFrame) -> DataFrame:
        print(self.ratings_data_source.source_path)
        print(self.users_data_source.source_path)
        ratings_df = self.session.read(self.ratings_data_source)
        users_df = self.session.read(self.users_data_source)
        ratings_df.show()
        users_df.show()
        df_source.show()
        return df_source
