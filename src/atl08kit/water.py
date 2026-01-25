from __future__ import annotations

import re
from pathlib import Path

# ATL08 filename pattern: ATL08_YYYYMMDD......
# used to extract YYYYMM for monthly masks
_ATL08_YYYYMM_RE = re.compile(r"ATL08_(\d{4})(\d{2})\d{2}")

def extract_yyyymm_from_atl08_name(name: str) -> str:
    # return YYYYMM from ATL08 filename or stem
    stem = Path(name).stem
    m = _ATL08_YYYYMM_RE.search(stem)
    if not m:
        raise ValueError(
            f"Cannot infer YYYYMM from ATL08 name: {name}. "
            "Expected pattern like 'ATL08_YYYYMMDD...'."
        )
    yyyy, mm = m.group(1), m.group(2)
    return f"{yyyy}{mm}"