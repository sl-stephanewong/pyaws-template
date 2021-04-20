import findspark

findspark.init()
from pyspark.sql import DataFrame
from src.utils.utils import *
from src.models.data_source import FileDataSource, DataFormat, DataSource, SourceKey
from ..aggregation_task import AggregationTask
from ..spark_session import Session

import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from sklearn.neighbors import NearestNeighbors


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
        return df_source
