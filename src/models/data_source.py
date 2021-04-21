from abc import ABC
from enum import Enum
from typing import Optional


class DataFormat(Enum):
    CSV = "csv"
    PARQUET = "parquet"
    JSON = "json"
    JDBC = "jdbc"


class DataSource(ABC):

    def __init__(self, data_format: DataFormat,
                 partition_number: int = 8,
                 options: Optional[dict] = None) -> None:
        super().__init__()
        self._data_format = data_format
        self._partition_number = partition_number
        self._options = options

    @property
    def partition_number(self):
        return self._partition_number

    @property
    def data_format(self):
        return self._data_format


class SourceKey(Enum):
    SRC_PATH = "source_path"
    DATA_FORMAT = "data_format"
    PART_NUMBER = "partition_number"
    PART_BY = "partition_by"
    MODE = "mode"

    # JDBC
    URL = "url"
    USER = "username"
    PWD = "password"
    DRIVER = "driver"
    TABLE = "table"


class FileDataSource(DataSource):

    # default format is parquet
    def __init__(self,
                 source_path: str,
                 data_format: DataFormat = DataFormat.PARQUET,
                 partition_number: int = 8,
                 partition_by: str = None,
                 mode: str = "append",
                 options: Optional[dict] = None) -> None:
        super(FileDataSource, self).__init__(data_format, partition_number, options)
        self.source_path = source_path
        self.partition_by = partition_by
        self.mode = mode
        self.options: Optional[dict] = options


class JDBCDataSource(DataSource):

    def get_properties(self) -> dict:
        return {"user": self.options.get(SourceKey.USER),
                "password": self.options.get(SourceKey.PWD)}

    def get_url(self) -> str:
        return self.options.get(SourceKey.URL)

    def __init__(self,
                 options: dict,
                 data_format: DataFormat = DataFormat.JDBC) -> None:
        super(JDBCDataSource, self).__init__(data_format, options=options)
