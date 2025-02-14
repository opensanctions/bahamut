from functools import lru_cache
from typing import Optional
from nomenklatura.util import iso_datetime


@lru_cache(maxsize=5000)
def datetime_ts(text: str) -> Optional[int]:
    dt = iso_datetime(text)
    if dt is not None:
        return int(dt.timestamp())
    return None
