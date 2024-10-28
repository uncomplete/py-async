import logging
import asyncio
import uuid
import decimal
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

import pyarrow as pa
import s3fs
from fsspec.implementations.local import LocalFileSystem
from strenum import StrEnum

log = logging.getLogger(__name__)

"""
TYPE_TO_TYPE dictionaries are used to map pyarrow types to python types to json types.
"""
PYTYPE_TO_PATYPE: Dict[type, pa.DataType] = {
    int: pa.int64(),
    float: pa.float64(),
    str: pa.string(),
    bool: pa.bool_(),
    decimal.Decimal: pa.float64()
}
PATYPE_TO_PYTYPE: Dict[pa.DataType, type] = {
    pa.int64(): int,
    pa.float64(): float,
    pa.string(): str,
    pa.bool_(): bool
}


def bool_cast(value: str = None) -> bool:
    return False if not value else value.lower() in ('true', '1')


async def cancel_all(tasks):
    # Entry and result queues are completely empty
    # We can safely cancel the worker tasks
    for task in tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            log.debug('Caught propagated task cancelled')
        finally:
            log.debug(f'Awaited task {task.get_name()}')


class TaskStatus(StrEnum):
    NOTSTARTED = 'NOTSTARTED'
    INPROGRESS = 'INPROGRESS'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'


@dataclass
class TaskDefinition:
    input_stream_id: uuid.UUID
    data: Dict[str, Any]
    status: TaskStatus = TaskStatus.NOTSTARTED
    id: uuid.UUID = uuid.uuid4()


class Stream(ABC):
    _id: uuid.UUID
    _queue: asyncio.Queue
    _task: asyncio.Task | None
    _stream: Any
    _schema: Optional[pa.Schema] = None

    def __init__(self):
        self._id = uuid.uuid4()
        self._task = None

    @abstractmethod
    async def consume(self):
        pass

    @abstractmethod
    def init_stream(self):
        pass

    def run(self):
        self.init_stream()
        if not self._task:
            self._task = asyncio.create_task(self.consume())
        else:
            raise ValueError('Stream is already running')

    @property
    def queue(self) -> asyncio.Queue:
        """The asyncio.Queue that the Stream uses to communicate with the worker."""
        return self._queue

def _choose_filesystem(path: str):
    if path.startswith('s3://'):
        return s3fs.S3FileSystem()
    else:
        return LocalFileSystem()


class InputStream(Stream, ABC):
    def __init__(self, *args, maxsize=1024, **kwargs):
        super().__init__(*args, **kwargs)
        self._queue = asyncio.Queue(maxsize=maxsize)

    def __iter__(self):
        return self

    @abstractmethod
    def __next__(self) -> Optional[Dict[str, Any]]:
        raise StopIteration

    async def consume(self):
        for message in self:
            await self._queue.put(
                TaskDefinition(
                    id=uuid.uuid4(),
                    input_stream_id=self._id,
                    data=message,
                ))


class OutputStream(Stream, ABC):
    def __init__(self, queue, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._queue = queue

    @abstractmethod
    def write(self, message: TaskDefinition):
        raise NotImplementedError

    async def consume(self):
        try:
            while True:
                td: TaskDefinition = await self._queue.get()
                if td.status not in (TaskStatus.COMPLETED, TaskStatus.FAILED):
                    log.error(
                        f"OutputStream {self._id} received task {td.id} with {td.status}.")
                else:
                    log.info(
                        f"OutputStream {self._id} received task {td.id} with {td.status}.")
                    log.debug(f'Result data: {td.data}')
                self.write(td)
                self._queue.task_done()
        except asyncio.CancelledError:
            log.info('Output task cancelled')
            raise
        except Exception as e:
            log.error(f'Output task finished with unexpected exception: {e}')
            raise
        finally:
            log.info('Output task finished')


class File(ABC):
    _path: str
    _fs: Any
    _file: Any
    _open: bool = False

    def __init__(self, path: str, force: bool = False, file_format: Optional[FileFormat] = None):
        self._path = path
        self._fs = _choose_filesystem(path)
        if force:
            if self._fs.exists(self._path):
                self._fs.rm(self._path)
        self._file = self.open()

    @abstractmethod
    def open(self) -> Any:
        raise NotImplementedError

    def close(self):
        if self._open:
            self._file.close()
            self._open = False

    def __del__(self):
        self.close()
