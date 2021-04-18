from ..models.data_source import *


def get_file_data_source(source: str, data_format: DataFormat = DataFormat.CSV) -> FileDataSource:
    return FileDataSource(
        source_path=source,
        data_format=data_format
    )