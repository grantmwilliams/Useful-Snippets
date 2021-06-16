"""
    This program takes a S3 URi a parquet file returns a lazy iterator of tuples to the values

    I have 3 implementations shown below. Benchmarking shows them to be similar in speed with 2 & 3 tending to be fastest.
"""
import s3fs
import pyarrow as pa
import pyarrow.parquet as pq


from itertools import chain
from typing import Tuple, Any

def iter_parquet(s3_uri: str, columns = None, batch_size=1_000) -> None:

    # create file system for file interface objects from S3
    fs = s3fs.S3FileSystem()

    # open a file interface object
    with fs.open(s3_uri) as fp:

        # convert the python file object into a ParquetFile object for iterating
        parquet_file = pq.ParquetFile(fp)

        # an iterator of pyarrow.RecordBatch
        record_batches = parquet_file.iter_batches(batch_size=batch_size, columns=columns)

        for batch in record_batches:
            columns = batch.columns
            pycols = []
            for col in columns:
                pycols.append(col.to_pylist())
            
            # convert from columnar to row format
            for row in zip(*pycols):
                yield row

def iter_parquet2(s3_uri: str, columns = None, batch_size=1_000) -> None:
    def _stream_from_record(record_batches: pa.RecordBatch):
        return chain.from_iterable(map(lambda batch: zip(*batch), record_batches))

    # create file system for file interface objects from S3
    fs = s3fs.S3FileSystem()

    # open a file interface object
    with fs.open(s3_uri) as fp:

        # convert the python file object into a ParquetFile object for iterating
        parquet_file = pq.ParquetFile(fp)

        # an iterator of pyarrow.RecordBatch
        record_batches = parquet_file.iter_batches(batch_size=batch_size, columns=columns)

        arrow_iter = _stream_from_record(record_batches)

        yield from (tuple(value.as_py() for value in row) for row in arrow_iter)

def iter_parquet3(s3_uri: str, columns = None, batch_size=1_000) -> Tuple[Any]:

    # create file system for file interface objects from S3
    fs = s3fs.S3FileSystem()

    # open a file interface object
    with fs.open(s3_uri) as fp:

        # convert the python file object into a ParquetFile object for iterating
        parquet_file = pq.ParquetFile(fp)

        # an iterator of pyarrow.RecordBatch
        record_batches = parquet_file.iter_batches(batch_size=batch_size, columns=columns)

        # convert from columnar format of pyarrow arrays to a row format of python objects (yields tuples)
        yield from chain.from_iterable(zip(*map(lambda col: col.to_pylist(), batch.columns)) for batch in record_batches)