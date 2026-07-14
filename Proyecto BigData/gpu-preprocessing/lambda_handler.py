import ctypes
import json
import os
import time

import numpy as np

LIB_PATH = os.environ.get("NORMALIZE_LIB_PATH", "./libnormalize.so")
lib = ctypes.CDLL(LIB_PATH)

lib.normalizeGPU.argtypes = [ctypes.POINTER(ctypes.c_float), ctypes.c_int, ctypes.c_float, ctypes.c_float]
lib.normalizeOMP.argtypes = [ctypes.POINTER(ctypes.c_float), ctypes.c_int, ctypes.c_float, ctypes.c_float]


def _run(values, minVal, maxVal, useGPU):
    arr = (ctypes.c_float * len(values))(*values)
    start = time.perf_counter()
    (lib.normalizeGPU if useGPU else lib.normalizeOMP)(arr, len(values), minVal, maxVal)
    elapsed = time.perf_counter() - start
    return list(arr), elapsed


def handler(event, context):
    body = json.loads(event.get("body", "{}")) if "body" in event else event
    values = body.get("data", [])

    if not values:
        return {"statusCode": 400, "body": json.dumps({"error": "El campo data es obligatorio y no puede estar vacio"})}

    npValues = np.array(values, dtype=np.float32)
    minVal, maxVal = float(npValues.min()), float(npValues.max())

    normalizedGPU, timeGPU = _run(values, minVal, maxVal, useGPU=True)
    _, timeOMP = _run(values, minVal, maxVal, useGPU=False)

    result = {
        "normalized": normalizedGPU,
        "min": minVal,
        "max": maxVal,
        "timings": {"gpuSeconds": timeGPU, "ompSeconds": timeOMP},
        "speedupGpuVsOmp": (timeOMP / timeGPU) if timeGPU > 0 else None,
    }
    return {"statusCode": 200, "body": json.dumps(result)}
