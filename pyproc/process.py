import asyncio
import logging
from dataclasses import dataclass, field
from functools import partial, wraps
from multiprocessing import Pipe
from multiprocessing import Process as _Process
from multiprocessing.connection import Connection
from typing import Any, Callable, Coroutine, Generic, ParamSpec, TypeVar, overload

logger = logging.getLogger()

P = ParamSpec("P")
R = TypeVar("R")

FunctionType = Callable[P, R]
CoroutineFunctionType = FunctionType[P, Coroutine[Any, Any, R]]


def async_wrap(func: FunctionType[P, R]):
    @wraps(func)
    async def run(*args: P.args, **kwargs: P.kwargs):
        loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(None, pfunc)

    return run


@dataclass(slots=True)
class Process(Generic[P, R]):
    _func: FunctionType[P, R] | CoroutineFunctionType[P, R]
    _result: R = field(init=False)
    _is_async: bool = field(init=False)

    _pid: int = -1
    _process: _Process = field(init=False)

    _parent_conn: Connection = field(init=False)
    _child_conn: Connection = field(init=False)

    def __post_init__(self):
        self._is_async = asyncio.iscoroutinefunction(self._func)

    @property
    def pid(self):
        return self._pid

    @property
    def process(self):
        return self._process

    @property
    def result(self: "Process[P, R]") -> R:
        if self._parent_conn and not self._parent_conn.closed:
            self._result = self._parent_conn.recv()
            self._parent_conn.close()

        return self._result

    def kill(self):
        self._process.kill()

    def join(self, timeout: float | None = None):
        self._process.join(timeout)

    def create_and_run(self, *args: P.args, **kwargs: P.kwargs):
        self._parent_conn, self._child_conn = Pipe()
        self._process = self._create_async() if self._is_async else self._create_sync()
        self._process.start()
        self._child_conn.close()  # we are only sending data from one end (the parent end)
        self._parent_conn.send((args, kwargs))
        self._pid = self._process.pid or -1
        return self._parent_conn

    def _create_async(self):
        async def execute(conn: Connection):
            try:
                args, kwargs = conn.recv()
                result = await self._func(*args, **kwargs)  # type: ignore
                conn.send(result)
            except Exception as error:
                logger.debug(error)
                raise error
            finally:
                conn.close()

        return _Process(target=lambda: (asyncio.run(execute(self._child_conn))))

    def _create_sync(self):
        def execute(conn: Connection):
            try:
                args, kwargs = conn.recv()
                result = self._func(*args, **kwargs)  # type: ignore
                conn.send(result)
            except Exception as error:
                logger.debug(error)
                raise error
            finally:
                conn.close()

        return _Process(target=lambda: (execute(self._child_conn)))

    @staticmethod
    def create_process_sync(function: FunctionType[P, R]):
        def proc(*args: P.args, **kwargs: P.kwargs) -> Process[P, R]:
            pc = Process(function)
            pc.create_and_run(*args, **kwargs)
            return pc

        return proc

    @staticmethod
    def create_process_async(function: CoroutineFunctionType[P, R]):
        async def proc(*args: P.args, **kwargs: P.kwargs) -> Process[P, R]:
            pc = Process(function)
            pc.create_and_run(*args, **kwargs)
            return pc

        return proc

    @overload
    @staticmethod
    def create_process(
        function: CoroutineFunctionType[P, R],
    ) -> CoroutineFunctionType[P, "Process[P, R]"]:
        ...

    @overload
    @staticmethod
    def create_process(
        function: FunctionType[P, R],
    ) -> FunctionType[P, "Process[P, R]"]:
        ...

    @staticmethod
    def create_process(function):
        if asyncio.iscoroutinefunction(function):
            return Process.create_process_async(function)

        return Process.create_process_sync(function)


@overload
def process(
    function: CoroutineFunctionType[P, R],
) -> CoroutineFunctionType[P, Process[P, R]]:
    ...


@overload
def process(
    function: FunctionType[P, R],
) -> FunctionType[P, Process[P, R]]:
    ...


def process(
    function,
):
    """
    Create a process worker. This function will run your function in a separate GIL
    """

    return Process.create_process(function)
