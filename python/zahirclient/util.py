from datetime import datetime
from functools import lru_cache
from typing import Optional
from nomenklatura.util import iso_datetime, datetime_iso


@lru_cache(maxsize=5000)
def datetime_ts(text: str) -> Optional[int]:
    dt = iso_datetime(text)
    if dt is not None:
        return int(dt.timestamp())
    return None


@lru_cache(maxsize=5000)
def ts_iso(epoch: int) -> Optional[str]:
    return datetime_iso(datetime.fromtimestamp(epoch))
