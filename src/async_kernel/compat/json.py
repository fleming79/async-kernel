from __future__ import annotations

import importlib.util
from typing import TYPE_CHECKING, Any

__all__ = ["pack_json_bytes", "pack_json_str", "unpack_json"]


if TYPE_CHECKING:
    from collections.abc import Callable

    pack_json_bytes: Callable[[Any, Callable[[Any], Any]], bytes]
    pack_json_str: Callable[[Any, Callable[[Any], Any]], str]
    unpack_json: Callable[[str | bytes], Any]


if importlib.util.find_spec("jupyter_client"):
    import json

    from jupyter_client.jsonutil import json_default

    def _jc_pack_bytes(data: Any, default=json_default) -> bytes:
        return json.dumps(data, default=default).encode()

    def _jc_pack_str(data: Any, default=json_default) -> str:
        return json.dumps(data, allow_nan=False, default=default)

    pack_json_bytes, pack_json_str, unpack_json = _jc_pack_bytes, _jc_pack_str, json.loads

else:
    json_default = None  # pragma: no cover


if importlib.util.find_spec("orjson"):
    import orjson

    ORJSON_OPTION = orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_NAIVE_UTC | orjson.OPT_UTC_Z

    def _oj_pack_bytes(data, default=json_default) -> bytes:
        return orjson.dumps(data, default=default, option=ORJSON_OPTION)

    def _oj_pack_str(data, default=json_default) -> str:
        return orjson.dumps(data, default=default, option=ORJSON_OPTION).decode()

    pack_json_bytes, pack_json_str, unpack_json = _oj_pack_bytes, _oj_pack_str, orjson.loads
