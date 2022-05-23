import abc
import itertools

from  typing import Any, Generator, Optional

import smart_open
import pyarrow as pa
import pyarrow.parquet as pq

__all__ = ["ParquetCursor"]


class ParquetDataStream(abc.ABC):
    """ Abstract Base Class for ParquetCursor

    Attributes:
        uri (str): URI of the parquet file
        mode (str): (optional) mode of the file interface object
        buffer_size (int): (optional) deserializing buffer size for remote files hosted on S3
        batch_size (int): (optional) maximum number of records to yield per batch
    """

    def __init__(self,
                 uri: str,
                 mode: str = "rb",
                 buffer_size: int = 1_000,
                 batch_size: int = 1_000):
        self.uri = uri
        self.mode = mode
        self.buffer_size = buffer_size if self.is_remote else 0 # buffer size set for remote only
        self.batch_size = batch_size
        self.file_buffer = smart_open.open(self.uri, self.mode)
        self.parquet_file = pq.ParquetFile(self.file_buffer, buffer_size=self.buffer_size, pre_buffer=self.is_remote)
        self.validate()
        self._record_batches = self.parquet_file.iter_batches(batch_size=self.batch_size, columns=self.columns)
        self._rows = self._get_row_generator()

    def __enter__(self) -> None:
        return self

    def __exit__(self) -> None:
        self.close()

    def __iter__(self) -> tuple[Any]:
        return self

    def __next__(self) -> tuple[Any]:
        return next(self._rows)

    def close(self) -> None:
        self.file_buffer.close()

    @property
    def is_remote(self) -> bool:
        return str.startswith(self.uri, "s3://")

    @property
    def num_rows(self) -> int:
        return self.parquet_file.metadata.num_rows

    @property
    def column_names(self) -> list[str]:
        return self.parquet_file.schema_arrow.names

    @abc.abstractmethod
    def _get_row_generator(self) -> Generator[tuple[Any], None, None]:
        pass

    @abc.abstractmethod
    def validate(self) -> None:
        pass


class ParquetCursor(ParquetDataStream):
    """ Lazy iterator over the rows of a parquet file

    Includes column filtering, prefetching, and buffering for remote files hosted on S3

    Attributes:
        uri (str): URI of the parquet file
        mode (str): (optional) mode of the file interface object
        columns (list[str]): (optional) list of columns to read from the parquet file if None, all columns are read
        schema (dict[str, pyarrow.DataType]): (optional) schema of the parquet file used to type check column return
                                              values if None, schema columns are not type checked (if only a subset
                                              of columns are defined, the schema only type checks the subset)
        buffer_size (int): (optional) deserializing buffer size for remote files hosted on S3
        batch_size (int): (optional) maximum number of records to yield per batch
    """

    def __init__(self,
                 uri: str,
                 mode: str = "rb",
                 columns: Optional[list[str]] = None,
                 schema: Optional[dict[str, pa.DataType]] = None,
                 buffer_size: int = 1_000,
                 batch_size: int = 1_000):

        self.columns = columns
        self.schema = schema
        ParquetDataStream.__init__(self, uri, mode, buffer_size, batch_size)

    def validate(self) -> None:
        if self.num_rows == 0:
            raise ValueError(f"Parquet file {self.uri} has no data rows")

        if self.columns is not None:
            columns = set(self.columns)
            column_names = set(self.column_names)

            extra_columns = sorted(list(columns - column_names))
            if len(extra_columns):
                raise ValueError(f"Parquet file {self.uri} does not contain columns {extra_columns}")

        if self.schema is not None:
            print(self.parquet_file.schema_arrow)
            for column, column_type in self.schema.items():
                if column not in self.column_names:
                    raise ValueError(f'Parquet file "{self.uri}" does not contain column "{column}"')
                schema_type = self.parquet_file.schema_arrow.field(column).type
                if column_type != schema_type:
                    raise ValueError(f'Column type for "{column}" is "{schema_type}" expected "{column_type}"')

    def _get_row_generator(self) -> Generator[tuple[Any], None, None]:
        return itertools.chain.from_iterable(zip(*map(lambda col: col.to_pylist(), batch.columns)) for batch in self._record_batches)
