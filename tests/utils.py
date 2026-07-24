from __future__ import annotations

import importlib.util
import os
from typing import TYPE_CHECKING, Any

import pytest

import async_kernel.utils
from async_kernel.typing import Message, MsgType, T
from tests.references import (
    RMessage,
    references,
)

if TYPE_CHECKING:
    from collections.abc import Callable

LAUNCHED_BY_DEBUGPY = async_kernel.utils.LAUNCHED_BY_DEBUGPY
TIMEOUT = 10 if not async_kernel.utils.LAUNCHED_BY_DEBUGPY else 1e6
MATPLOTLIB_INLINE_BACKEND = "module://matplotlib_inline.backend_inline"

CI = bool(os.environ.get("CI", "False"))
CI_DEBUGGING = bool(os.environ.get("CI_DEBUGGING"))


def validate_message(msg: Message, msg_type: MsgType | None = None, parent=""):
    """Validate a message.

    If msg_type and/or parent are given, the msg_type and/or parent msg_id
    are compared with the given values.
    """
    RMessage().check(msg)

    if msg_type and msg["header"]["msg_type"] != msg_type:
        msg_ = f"Expected {msg_type=} but got '{msg['header']['msg_type']}'  for {msg=}"
        raise ValueError(msg_)
    if parent and (msg.get("parent_header") or {}).get("msg_id") != parent:
        msg_ = f"This parent 'msg_id' does not match {msg=} {parent=}"
        raise RuntimeError(msg_)
    content = msg["content"]
    ref = references[msg["header"]["msg_type"]]
    try:
        ref.check(content)
    except Exception as e:
        e.add_note(f"\n{msg_type=}\n{parent=}\n{content=}")
        raise


def check_pub_message(msg: Message, *, msg_type=MsgType.iopub_status, **content_checks):
    "Check an iopub message for particular content."
    if msg["header"]["msg_type"] != msg_type:
        msg_ = f"""msg_type missmate {msg_type} but got {msg["header"]["msg_type"]}"""
        err = ValueError(msg_)
        err.add_note(f"{msg=}")
        raise err
    content = msg["content"]
    for k, v in content_checks.items():
        if content[k] != v:
            msg_ = f"Failed content check for {msg_type=}  expected: {content.get(k)!r} got: {v!r}"
            raise ValueError(msg_)
    return msg


def skip_if_missing(name: str, reason="") -> Callable[[T], Callable[[T], Any]]:
    """Skip the test if the module is not installed."""
    # Inspiration: https://github.com/pytest-dev/pytest/discussions/13140#discussioncomment-11869648
    if not importlib.util.find_spec(name):
        return pytest.mark.skip(reason or f"{name} missing")
    return lambda func: func  # pyright: ignore[reportReturnType]
