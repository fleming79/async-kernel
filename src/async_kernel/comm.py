from __future__ import annotations

from typing import Self

import comm
from comm.base_comm import BaseComm, BuffersType, MaybeDict
from traitlets import Dict, HasTraits, Instance, observe
from typing_extensions import override

from async_kernel import Kernel

__all__ = ["Comm"]


class Comm(BaseComm):
    """
    An implementation of `comm.BaseComms` for async-kernel.

    !!! note

        - `kernel` is added/removed by the CommManager.
        - `kernel` is added to the CommManager by the kernel once the sockets have been opened.
        - publish_msg is no-op when kernel is unset.
    """

    __slots__ = [
        "_close_callback",
        "_close_data",
        "_closed",
        "_msg_callback",
        "_open_data",
        "comm_id",
        "primary",
        "target_module",
        "target_name",
        "topic",
    ]
    kernel: Kernel | None = None

    @override
    def publish_msg(
        self,
        msg_type: str,
        data: MaybeDict = None,
        metadata: MaybeDict = None,
        buffers: BuffersType = None,
        **keys,
    ):
        """Helper for sending a comm message on IOPub."""
        if (kernel := self.kernel) is None:
            # Only send when the kernel is set
            return
        content = {"data": {} if data is None else data, "comm_id": self.comm_id} | keys
        kernel.iopub_send(
            msg_or_type=msg_type,
            content=content,
            metadata=metadata,
            parent=None,
            ident=self.topic,
            buffers=buffers,
        )

    @override
    def handle_msg(self, msg: comm.base_comm.MessageType) -> None:
        """Handle a comm_msg message"""
        if self._msg_callback:
            self._msg_callback(msg)


class CommManager(HasTraits, comm.base_comm.CommManager):  # pyright: ignore[reportUnsafeMultipleInheritance]
    """
    The comm manager for all Comm instances.

    !!! note

        - When the trait `CommManager.kernel` is set the `Comm.kernel` trait is set on all [async_kernel.comm.Comm][] instances.
        - The `Comm` will only send messages when the kernel is set.
        - The `kernel` sets `CommManager.kernel` when its ready the iopub socket is open.
    """

    _instance = None
    kernel: Instance[Kernel | None] = Instance(Kernel, allow_none=True)  # pyright: ignore[reportAssignmentType]
    comms: Dict[str, BaseComm] = Dict()  # pyright: ignore[reportIncompatibleVariableOverride]
    targets: Dict[str, comm.base_comm.CommTargetCallback] = Dict()  # pyright: ignore[reportIncompatibleVariableOverride]

    def __new__(cls) -> Self:
        if cls._instance:
            return cls._instance
        cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        super().__init__()

    @observe("kernel")
    def _observe_kernel(self, change: dict):
        kernel: Kernel = change["new"]
        for c in self.comms.values():
            if isinstance(c, Comm):
                c.kernel = kernel

    @override
    def register_comm(self, comm: comm.base_comm.BaseComm) -> str:
        """Register a new comm"""
        if isinstance(comm, Comm) and (kernel := self.kernel):
            comm.kernel = kernel
        return super().register_comm(comm)


comm_manager = CommManager()


def get_comm_manager():
    return comm_manager


def set_comm():
    """
    Monkey patch the [comm](https://pypi.org/project/comm/) module's functions to provide iopub comms.

    1.  `comm.create_comm` to [async_kernel.comm.Comm][].
    1. `comm.get_com_manager` to [async_kernel.comm.CommManager][].
    """
    comm.create_comm = Comm
    comm.get_comm_manager = get_comm_manager
