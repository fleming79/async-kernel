from __future__ import annotations

import importlib.util
from typing import TYPE_CHECKING

import pytest

from async_kernel.compat import json as compat_json

if TYPE_CHECKING:
    from async_kernel.typing import Job


class TestJson:
    @pytest.mark.skipif(not importlib.util.find_spec("orjson"), reason="orjson not installed")
    def test_oj_pack_bytes(self, job: Job) -> None:
        packed = compat_json._oj_pack_bytes(job["msg"])  # pyright: ignore[reportPrivateUsage]
        assert isinstance(packed, bytes)
        msg = compat_json.unpack_json(packed)
        assert msg == job["msg"]

    @pytest.mark.skipif(not importlib.util.find_spec("orjson"), reason="orjson not installed")
    def test_oj_pack_str(self, job: Job) -> None:
        packed = compat_json._oj_pack_str(job["msg"])  # pyright: ignore[reportPrivateUsage]
        assert isinstance(packed, str)
        msg = compat_json.unpack_json(packed)
        assert msg == job["msg"]

    def test_jc_pack_bytes(self, job: Job) -> None:
        packed = compat_json._jc_pack_bytes(job["msg"])  # pyright: ignore[reportPrivateUsage]
        assert isinstance(packed, bytes)
        msg = compat_json.unpack_json(packed)
        assert msg == job["msg"]

    def test_jc_pack_str(self, job: Job) -> None:
        packed = compat_json._jc_pack_str(job["msg"])  # pyright: ignore[reportPrivateUsage]
        assert isinstance(packed, str)
        msg = compat_json.unpack_json(packed)
        assert msg == job["msg"]
