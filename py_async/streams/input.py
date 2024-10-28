import csv
import decimal
import re
import logging
import io
from typing import Any

import ijson
import pyarrow
import pyarrow.parquet as pq

from py_async.streams.core import File, InputStream, PYTYPE_TO_PATYPE, PATYPE_TO_PYTYPE

log = logging.getLogger(__name__)

FLOAT_RE = re.compile(r'^([+\-])?(\d+(\.\d*)?|\.\d+)$')
INT_RE = re.compile(r'^\d+$')
BOOL_RE = re.compile(r'^(true|false)$', re.IGNORECASE)


def create_iterator(lst):
    for x in lst:
        yield x


class IParquet(File, InputStream):
    """
    Class for reading streams from Parquet files.
    """

    _batch_iter: Any = None
    _record_iter: Any = None

    def open(self) -> pyarrow.parquet.ParquetFile:
        log.info(f"Reading Parquet file: `{self._path}`")
        return pq.ParquetFile(
            self._path,
            filesystem=self._fs
        )

    def init_stream(self):
        self._batch_iter = self._file.iter_batches()
        self._record_iter = create_iterator(next(self._batch_iter).to_pylist())
        return

    def __next__(self):
        row = None
        try:
            row = next(self._record_iter)
        except StopIteration:
            try:
                self._record_iter = create_iterator(next(self._batch_iter).to_pylist())
                row = next(self._record_iter)
            except StopIteration:
                raise StopIteration
        return row

    def get_columns(self):
        return {k: v for v, k in enumerate(self._file.schema.names)}

    def get_types(self):
        return {k: v for k, v in zip(self._file.schema.names, self._file.schema.to_arrow_schema().types)}


class IJson(File, InputStream):
    """
    Class to iterate over JSON files.
    """

    _file = None
    _iter = None
    _column_indices = None
    _column_types = None
    _first = True
    _first_data = None

    def open(self) -> Any:
        log.info(f"Reading JSON file: `{self._path}`")
        return self._fs.open(self._path, 'rb')

    def init_stream(self):
        self._iter = ijson.items(self._file, f'item')
        self._first_data = next(self._iter)
        self._column_indices = {k: v for v, k in enumerate(self._first_data.keys())}
        self._column_types = {}
        self._column_types = {k: PYTYPE_TO_PATYPE[type(v)] for k, v in self._first_data.items()}

    def __next__(self):
        row = next(self._iter) if not self._first else self._first_data
        self._first = False
        ret = {}
        for k, v in row.items():
            if isinstance(v, decimal.Decimal):
                ret[k] = int(v) if v.as_integer_ratio()[1] == 1 else float(v)
            else:
                ret[k] = v
        return ret

    def get_columns(self):
        return self._column_indices

    def get_types(self):
        return self._column_types


class ICsv(File, InputStream):
    """
    Class to iterate over CSV files.
    """

    _file = None
    _iter = None
    _column_indices = None
    _column_types = None
    _file_wrapper = None
    _first = True
    _first_data = None

    def open(self) -> Any:
        log.info(f"Reading CSV file: `{self._path}`")
        return io.TextIOWrapper(self._fs.open(self._path, 'rb'), encoding='utf-8')

    def init_stream(self):
        self._iter = csv.reader(self._file)
        # Read the first row to get the column names
        column_names = next(self._iter)
        # Read the second row to get the column types
        self._first_data = next(self._iter)
        # Create a dictionary mapping column names to column indices
        self._column_indices = {column_names[i]: i for i in range(len(column_names))}
        self._column_types = self._infer_types(self._first_data)

    def __next__(self):
        row = next(self._iter) if not self._first else self._first_data
        self._first = False
        ret = {}
        for k, v in self._column_indices.items():
            if v >= len(row):
                log.error(f"Column {k} has no value at index {v}.")
            elif row[v] == '':
                ret[k] = PATYPE_TO_PYTYPE[self._column_types[k]]()
            else:
                ret[k] = PATYPE_TO_PYTYPE[self._column_types[k]](row[v])
        return ret

    def get_columns(self):
        return self._column_indices

    def get_types(self):
        return self._column_types

    def _infer_types(self, row):
        column_types = {}
        for k, v in zip(self._column_indices.keys(), row):
            if INT_RE.match(v):
                log.debug(f"Detected int value: {v}")
                column_types[k] = pyarrow.int64()
            elif FLOAT_RE.match(v):
                log.debug(f"Detected float value: {v}")
                column_types[k] = pyarrow.float64()
            elif BOOL_RE.match(v):
                log.debug(f"Detected bool value: {v}")
                column_types[k] = pyarrow.bool_()
            else:
                log.debug(f"Detected string value: {v}")
                column_types[k] = pyarrow.string()
        return column_types
