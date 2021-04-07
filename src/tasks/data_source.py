from enum import Enum


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
