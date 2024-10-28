import csv
import io
import json
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.parquet as pq

from py_async.streams.core import File, OutputStream


class OParquet(File, OutputStream):
    _batch_size: int = 1024
    _record_buffer: List[Any] = []
    _record_index: int = 0

    def __init__(self, *args, batch_size=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._batch_size = batch_size | self._batch_size

    def open(self) -> pq.ParquetWriter:
        return pq.ParquetWriter(
            self._path,
            self._schema,
            filesystem=self._fs
        )

    def write(self, record: Dict):
        self._file.write(record)

    def init_stream(self):
        return

    def close(self):
        self._flush_buffer()
        super().close()

    def __del__(self):
        self.close()

    def _flush_buffer(self):
        if self._record_index == 0:
            return
        self._file.write_batch(pa.RecordBatch.from_pylist(self._record_buffer, schema=self._schema))
        self._record_buffer = []
        self._record_index = 0


class OCsv(File, OutputStream):
    _writer: csv.writer = None

    def open(self) -> io.TextIOWrapper:
        return io.TextIOWrapper(self._fs.open(self._path, 'wb'), encoding='utf-8')

    def init_stream(self):
        self._writer = csv.writer(self._file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        self._writer.writerow(self._schema.names)
        return

    def write(self, record: Dict):
        self._writer.writerow([record[k] for k in self._schema.names])

    def close(self):
        return


class OJson(File, OutputStream):
    _first_record: bool = True
    _open: bool = False

    def open(self) -> io.TextIOWrapper:
        return io.TextIOWrapper(self._fs.open(self._path, 'wb'), encoding='utf-8')

    def init_stream(self):
        self._first_record = True
        self._open = True
        self._file.write('[')
        return

    def write(self, record: Dict):
        if not self._open:
            return
        if self._first_record:
            self._first_record = False
        else:
            self._file.write(',')
        self._file.write(json.dumps(record))

    def close(self):
        if not self._open:
            return
        self._open = False
        self._file.write(']')
        return
