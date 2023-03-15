from importlib.metadata import version

from pyproc.process import Process, process

__version__ = version("python-pyproc")
__all__ = ["Process", "process"]
