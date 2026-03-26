from __future__ import annotations

import importlib.util
from typing import Any

__all__ = ["dump_bytes", "dump_string", "loads"]


if importlib.util.find_spec("jupyter_client"):
    import json

    from jupyter_client.jsonutil import json_default

    def _jc_dump_bytes(data: Any) -> bytes:
        return json.dumps(data, default=json_default).encode()

    def _jc_dump_string(data: Any) -> str:
        return json.dumps(data, default=json_default)

    dump_bytes, dump_string, loads = _jc_dump_bytes, _jc_dump_string, json.loads

else:
    json_default = None  # pragma: no cover


if importlib.util.find_spec("orjson"):
    import orjson

    ORJSON_OPTION = orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_NAIVE_UTC | orjson.OPT_UTC_Z

    def _oj_dump_bytes(data) -> bytes:
        return orjson.dumps(data, default=json_default, option=ORJSON_OPTION)

    def _oj_dump_string(data) -> str:
        return orjson.dumps(data, default=json_default, option=ORJSON_OPTION).decode()

    dump_bytes, dump_string, loads = _oj_dump_bytes, _oj_dump_string, orjson.loads
