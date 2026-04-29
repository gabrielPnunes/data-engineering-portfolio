# src/logger.py

import os
from datetime import datetime


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LEVELS    = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3}


def log(step: str, msg: str, level: str = "INFO") -> None:
    if LEVELS.get(level, 1) >= LEVELS.get(LOG_LEVEL, 1):
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] [{level}] [{step}] {msg}")


def log_error(step: str, e: Exception) -> None:
    import traceback
    log(step, str(e), level="ERROR")
    traceback.print_exc()