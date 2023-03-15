# python-worker

## Description
Turn a function into a process with a simple decorator.

This repo is highly inspired by [python-worker](https://github.com/Danangjoyoo/python-worker)

---

## Installation
```bash
pip install python-pyproc
```

---
## Guide
```python
from pyproc import process

@process
def func(x: int) -> int:
    ...

func(123)

@process
async def func(x : int) -> int:
    ...

await func(123)
```

---
## Get Return Value

```python
import time

@process
def func(a: int) -> int:
    # do some work
    time.sleep(5)
    return a + 1

worker = func(10)

# this will await the worker to finished and return the value
worker.result  # 11 will return after 5 seconds
worker.result  # 11 no wait this time
```
---
### Notes
  Only Pass Pickable Types!. Passing Non Pickable Types Like Genrators Will Cause An Error
