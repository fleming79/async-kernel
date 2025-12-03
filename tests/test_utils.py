from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import pytest
from traitlets import HasTraits, Instance, Int, default

from async_kernel import utils as ak_utils
from async_kernel.typing import Tags

if TYPE_CHECKING:
    from async_kernel.typing import ExecuteContent, Job


@pytest.mark.anyio
class TestUtils:
    async def test_get_job(self, job: Job[ExecuteContent]) -> None:
        with pytest.raises(LookupError):
            ak_utils._job_var.get()  # pyright: ignore[reportPrivateUsage]
        ak_utils.get_job()
        ak_utils._job_var.set(job)  # pyright: ignore[reportPrivateUsage]
        assert ak_utils.get_job() is job

    async def test_get_execution_count(self, job: Job[ExecuteContent]):
        assert ak_utils.get_execution_count() == 0

    async def test_get_metadata(self, job: Job[ExecuteContent]):
        assert ak_utils.get_metadata() == {}
        assert ak_utils.get_metadata(job) is job["msg"]["metadata"]
        ak_utils._job_var.set(job)  # pyright: ignore[reportPrivateUsage]
        assert ak_utils.get_metadata() is job["msg"]["metadata"]

    async def test_get_parent(self, job: Job[ExecuteContent]):
        assert ak_utils.get_parent() is None
        assert ak_utils.get_parent(job) is job["msg"]
        ak_utils._job_var.set(job)  # pyright: ignore[reportPrivateUsage]
        assert ak_utils.get_parent(job) is job["msg"]

    async def test_get_tags(self, job: Job[ExecuteContent]):
        job["msg"]["metadata"]["tags"] = tags = []
        assert ak_utils.get_tags() == []
        assert ak_utils.get_tags(job) is tags

    @pytest.mark.parametrize(
        ("tag", "default", "str_tag", "expected_value"),
        [
            (Tags.timeout, 0.0, "timeout=1.3", 1.3),  # float
            (Tags.timeout, 0.0, "", 0.0),  # float
            (Tags.timeout, 0, "timeout=0.0", 0),  # int
            (Tags.stop_on_error, True, "stop_on_error", True),  # bool
            (Tags.suppress_error, "", "suppress-error=Msg", "Msg"),  # str
            (Tags.raises_exception, True, "raises-exception", True),  # bool
            (Tags.raises_exception, True, "raises-exception=false", False),  # bool
            (Tags.raises_exception, True, "raises-exception=False", False),  # bool
            (Tags.raises_exception, False, "raises-exception=True", True),  # bool
        ],
    )
    async def test_get_tag_value(self, tag: Tags, default: Any, str_tag: str, expected_value: Any):
        value = ak_utils.get_tag_value(tag, default, tags=[str_tag])
        assert value == expected_value
        assert type(value) is type(expected_value)

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
