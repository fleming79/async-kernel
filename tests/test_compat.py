from __future__ import annotations

import importlib.util
from typing import TYPE_CHECKING

import pytest

from async_kernel.compat import json as compat_json

if TYPE_CHECKING:
    from async_kernel.typing import Job


class TestJson:
    @pytest.mark.skipif(not importlib.util.find_spec("orjson"), reason="orjson not installed")
    def test_oj_dump_bytes(self, job: Job) -> None:
        packed = compat_json._oj_dump_bytes(job["msg"])  # pyright: ignore[reportPrivateUsage]
        assert isinstance(packed, bytes)
        msg = compat_json.loads(packed)
        assert msg == job["msg"]

    @pytest.mark.skipif(not importlib.util.find_spec("orjson"), reason="orjson not installed")
    def test_oj_dump_string(self, job: Job) -> None:
        packed = compat_json._oj_dump_string(job["msg"])  # pyright: ignore[reportPrivateUsage]
        assert isinstance(packed, str)
        msg = compat_json.loads(packed)
        assert msg == job["msg"]

    def test_jc_dump_bytes(self, job: Job) -> None:
        packed = compat_json._jc_dump_bytes(job["msg"])  # pyright: ignore[reportPrivateUsage]
        assert isinstance(packed, bytes)
        msg = compat_json.loads(packed)
        assert msg == job["msg"]

    def test_jc_dump_string(self, job: Job) -> None:
        packed = compat_json._jc_dump_string(job["msg"])  # pyright: ignore[reportPrivateUsage]
        assert isinstance(packed, str)
        msg = compat_json.loads(packed)
        assert msg == job["msg"]
