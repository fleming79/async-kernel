from __future__ import annotations

import threading
from typing import TYPE_CHECKING, cast

import pytest
from traitlets import HasTraits, Instance, Int, default

from async_kernel import utils as ak_utils

if TYPE_CHECKING:
    from async_kernel.typing import ExecuteContent, Job


class TestUtils:
    def test_do_not_debug_this_thread(self):
        with ak_utils.do_not_debug_this_thread():
            thread = threading.current_thread()
            assert getattr(thread, "pydev_do_not_trace", None)

    async def test_get_job(self, anyio_backend, job: Job[ExecuteContent]) -> None:
        with pytest.raises(LookupError):
            ak_utils._job_var.get()  # pyright: ignore[reportPrivateUsage]
        ak_utils.get_job()
        ak_utils._job_var.set(job)  # pyright: ignore[reportPrivateUsage]
        assert ak_utils.get_job() is job

    async def test_get_execution_count(self, anyio_backend, job: Job[ExecuteContent]):
        assert ak_utils.get_execution_count() == 0
        ak_utils._execution_count_var.set(3)  # pyright: ignore[reportPrivateUsage]
        assert ak_utils.get_execution_count() == 3

    async def test_get_metadata(self, anyio_backend, job: Job[ExecuteContent]):
        assert ak_utils.get_metadata() == {}
        assert ak_utils.get_metadata(job) is job["msg"]["metadata"]
        ak_utils._job_var.set(job)  # pyright: ignore[reportPrivateUsage]
        assert ak_utils.get_metadata() is job["msg"]["metadata"]

    async def test_get_parent(self, anyio_backend, job: Job[ExecuteContent]):
        assert ak_utils.get_parent() is None
        assert ak_utils.get_parent(job) is job["msg"]
        ak_utils._job_var.set(job)  # pyright: ignore[reportPrivateUsage]
        assert ak_utils.get_parent(job) is job["msg"]

    async def test_get_tags(self, anyio_backend, job: Job[ExecuteContent]):
        job["msg"]["metadata"]["tags"] = tags = []  # pyright: ignore[reportIndexIssue]
        assert ak_utils.get_tags() == []
        assert ak_utils.get_tags(job) is tags

    async def test_get_execute_request_timeout(self, anyio_backend, job: Job[ExecuteContent]):
        job["msg"]["metadata"] = {"timeout": 3}
        assert ak_utils.get_execute_request_timeout(job) == 3
        ak_utils._job_var.set(job)  # pyright: ignore[reportPrivateUsage]
        assert ak_utils.get_execute_request_timeout() == 3

    def test_setattr_nested(self):
        class TestObj:
            k = None
            nested: TestObj

        test_obj = TestObj()
        test_obj.nested = TestObj()

        # Directly set
        ak_utils.setattr_nested(test_obj, "k", "1")
        assert test_obj.k == "1"
        # Nested
        ak_utils.setattr_nested(test_obj, "nested.k", 2)
        assert test_obj.nested.k == 2
        # Does not set a missing attribute
        ak_utils.setattr_nested(test_obj, "not_an_attribute", None)
        assert not hasattr(test_obj, "not_an_attribute")

    def test_setattr_nested_has_traits(self):
        class TestObj(HasTraits):
            k = Int()
            nested = Instance(HasTraits)
            nested_with_default = Instance(cast("type[TestObj]", HasTraits))

            @default("nested_with_default")
            def _default_nested_with_default(self):
                return TestObj()

        test_obj = TestObj()
        # Set with cast
        ak_utils.setattr_nested(test_obj, "k", "1")
        assert test_obj.k == 1
        # Handles missing traits
        ak_utils.setattr_nested(test_obj, "nested.k", "2")
        assert not test_obj.trait_has_value("nested")
        # Sets nested trait with a default
        ak_utils.setattr_nested(test_obj, "nested_with_default.k", "2")
        assert test_obj.nested_with_default.k == 2
