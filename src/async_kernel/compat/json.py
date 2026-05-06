from __future__ import annotations

import importlib.util
from typing import TYPE_CHECKING, Any

__all__ = ["pack_json_bytes", "pack_json_str", "unpack_json"]


if TYPE_CHECKING:
    from collections.abc import Callable

    from jupyter_client.jsonutil import json_default

    def pack_json_bytes(obj: Any, /, default: Callable[[Any], Any] | None = json_default) -> bytes:
        """
        Pack obj into json serialized bytes.

        Args:
            data: The data to serialize.
            default: A function that should return a serializable version of obj or raise TypeError.
        """
        ...

    def pack_json_str(obj: Any, /, default: Callable[[Any], Any] | None = json_default) -> str:
        """
        Pack obj into json serialized string.

        Args:
            data: The data to serialize.
            default: A function that should return a serializable version of obj or raise TypeError.
        """
        ...

    def unpack_json(data: str | bytes, /) -> Any:
        "Deserialize data in a Python object."
        ...


if importlib.util.find_spec("jupyter_client"):
    import json

    from jupyter_client.jsonutil import json_default

    def _jc_pack_bytes(obj: Any, default: Callable[[Any], Any] | None = json_default) -> bytes:
        return json.dumps(obj, default=default).encode()

    def _jc_pack_str(obj: Any, default: Callable[[Any], Any] | None = json_default) -> str:
        return json.dumps(obj, allow_nan=False, default=default)

    pack_json_bytes, pack_json_str, unpack_json = _jc_pack_bytes, _jc_pack_str, json.loads

else:
    json_default = None  # pragma: no cover


if importlib.util.find_spec("orjson"):
    import orjson

    ORJSON_OPTION = orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_NAIVE_UTC | orjson.OPT_UTC_Z

    def _oj_pack_bytes(obj: Any, default: Callable[[Any], Any] | None = json_default) -> bytes:
        return orjson.dumps(obj, default=default, option=ORJSON_OPTION)

    def _oj_pack_str(obj: Any, default: Callable[[Any], Any] | None = json_default) -> str:
        return orjson.dumps(obj, default=default, option=ORJSON_OPTION).decode()

    pack_json_bytes, pack_json_str, unpack_json = _oj_pack_bytes, _oj_pack_str, orjson.loads
