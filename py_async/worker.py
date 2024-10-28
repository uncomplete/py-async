import asyncio
import logging
import time
import uuid
from abc import abstractmethod, ABC
from asyncio import FIRST_EXCEPTION
from dataclasses import dataclass
from typing import Any, Self

from py_async.streams.core import InputStream, OutputStream, TaskStatus, \
    TaskDefinition, cancel_all

log = logging.getLogger(__name__)


@dataclass
class TaskResult:
    data: Any
    status: TaskStatus = TaskStatus.INPROGRESS


@dataclass
class WorkerContext:
    context: Any
    worker_id: uuid.UUID = uuid.uuid4()


class Worker(ABC):
    _input_stream: InputStream
    _output_stream: OutputStream

    @abstractmethod
    async def process(self, data: Any, context: WorkerContext) -> TaskResult:
        pass

    @abstractmethod
    def worker_init(self) -> WorkerContext:
        pass

    def input(self, stream: InputStream) -> Self:
        self._input_stream = stream
        return self

    def output(self, stream: OutputStream) -> Self:
        self._output_stream = stream
        return self

    async def consume(self):
        worker_context = self.worker_init()
        worker_id = uuid.uuid4()
        log.debug(f'Worker {worker_id} started.')
        count = 0
        task_id = None
        while True:
            try:
                count += 1
                task_id = None
                _time = time.time()
                task: TaskDefinition = await self._input_stream.queue.get()
                task_id = task.id
                log.debug(f'Worker {worker_id} processing task {task.id}. Waited {time.time() - _time}.')
                _time = time.time()
                result: TaskResult = await self.process(task.data, worker_context)
                log.debug(f'Worker {worker_id} processed task {task.id} with {result.status} in {time.time() - _time}.')
                await self._output_stream.queue.put(result.data)
                log.debug(f'Worker {worker_id} put task {task.id}.')
            except asyncio.CancelledError:
                log.info(f'Worker {worker_id} cancelled')
                raise
            except Exception as e:
                log.error(f'Worker {worker_id} finished {task_id} with unexpected exception: {e}.')
            finally:
                # Call task_done on the task
                self._input_stream.queue.task_done()
                log.debug(f'Worker {worker_id} finished {task_id} successfully.')

    async def run(self, num_workers: int = 1):
        input_tasks = [asyncio.create_task(self._input_stream.consume())]
        worker_tasks = []
        for i in range(num_workers):
            worker_tasks.append(asyncio.create_task(self.consume()))
        output_tasks = [asyncio.create_task(self._output_stream.consume())]
        pending = [*input_tasks, *worker_tasks, *output_tasks]
        input_tasks_done = 0
        success = True
        while pending:
            try:
                done, pending = await asyncio.wait(pending, timeout=2, return_when=FIRST_EXCEPTION)
                for t in done:
                    r = t.result()
                    log.info(f'Task {t.get_name()} finished with {r}')
                    if t in input_tasks:
                        input_tasks_done += 1
                if input_tasks_done == len(input_tasks):
                    break
            except Exception as e:
                log.error(f"Unexpected exception: {e}")
                success = False
                break

        if not success:
            # If any input, worker, or output task fails with an unexpected exception
            #  we must cancel all tasks to prevent hanging of the entire process.
            await cancel_all(pending)
            log.error('Finished with Error.')
            return

        # All input tasks are done.
        # Wait until the queues are empty.
        await self._input_stream.queue.join()
        log.info("Gathered input queue task")
        await self._output_stream.queue.join()
        log.info("Gathered output queue task")

        # Given success, worker tasks must be cancelled but only once queues are empty.
        await cancel_all(worker_tasks)
        log.info('Gathered worker tasks')

        # Result queue is completely empty, workers are not adding anymore tasks
        # We can safely cancel the output task
        await cancel_all({output_tasks})
        log.info('Gathered output tasks')

        log.info('Finished with Success.')
        return
