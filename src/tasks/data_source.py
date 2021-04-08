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
                 options: Optional[dict] = None) -> None:
        super().__init__()
        self.data_format = data_format
        self.options = options


class FileDataSource(DataSource):

    # default format is parquet
    def __init__(self,
                 source_path: str,
                 data_format: DataFormat = DataFormat.PARQUET,
                 partition_number: int = 8,
                 partition_by: str = None,
                 mode: str = "append",
                 options: Optional[dict] = None) -> None:
        super(FileDataSource, self).__init__(data_format, options)
        self.source_path = source_path
        self.partition_number = partition_number
        self.partition_by = partition_by
        self.mode = mode
        self.options: Optional[dict] = options


class JDBCDataSourceKey(Enum):
    URL = "url"
    USERNAME = "username"
    PWD = "password"
    TABLE = "table"


class JDBCDataSource(DataSource):

    def get_properties(self) -> dict:
        return {"user": self.options.get(JDBCDataSourceKey.USERNAME),
                "password": self.options.get(JDBCDataSourceKey.PWD)}

    def get_url(self) -> str:
        return self.options.get(JDBCDataSourceKey.URL)

    def __init__(self,
                 options: dict,
                 data_format: DataFormat = DataFormat.JDBC) -> None:
        super(JDBCDataSource, self).__init__(data_format, options)
