from __future__ import annotations

import contextlib
import sys
from typing import TYPE_CHECKING, Self

import comm
from aiologic.meta import import_module
from comm.base_comm import BaseComm, BuffersType, MaybeDict
from typing_extensions import override

from async_kernel.common import Fixed
from async_kernel.interface import HasInterface

if TYPE_CHECKING:
    from async_kernel.kernel import Kernel

__all__ = ["Comm"]


class Comm(HasInterface, BaseComm):
    """
    An implementation of `comm.BaseComms` for async-kernel  ([on pypi](https://pypi.org/project/comm/)).

    Notes:
        - `kernel` is added/removed by the CommManager.
        - `kernel` is added to the CommManager by the kernel once the sockets have been opened.
        - publish_msg is no-op when kernel is unset.
    """

    kernel: Fixed[Self, Kernel] = Fixed(lambda c: c["owner"].parent.kernel, use_weakref=True)

    @override
    def publish_msg(
        self,
        msg_type: str,
        data: MaybeDict = None,
        metadata: MaybeDict = None,
        buffers: BuffersType = None,
        **keys,
    ) -> None:
        """Helper for sending a comm message on IOPub."""
        content = {"data": {} if data is None else data, "comm_id": self.comm_id} | keys
        self.parent.iopub_send(
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


class CommManager(HasInterface, comm.base_comm.CommManager):
    """
    The comm manager for all Comm instances.

    There is only one instance, whose lifetime is linked to the Kernel.
    """

    kernel: Fixed[Self, Kernel] = Fixed(lambda c: c["owner"].parent.kernel, use_weakref=True)

    comms: dict[str, BaseComm]
    targets: dict[str, comm.base_comm.CommTargetCallback]

    def patch_comm(self) -> None:
        """
        Monkey patch the [comm](https://pypi.org/project/comm/) module's functions to provide iopub comms.

        1.  `comm.create_comm` to [Comm][].
        1. `comm.get_com_manager` to [CommManager][].

        Also patches ipykernel.comm if ipykernel is installed.
        """
        comm.create_comm = Comm
        comm.get_comm_manager = lambda: self

        if sys.platform != "emscripten":
            # Monkey patch ipykernel in case other libraries use its comm module directly. Eg: pyviz_comms:https://github.com/holoviz/pyviz_comms/blob/4cd44d902364590ba8892c8e7f48d7888d0a1c0c/pyviz_comms/__init__.py#L403C14-L403C28
            with contextlib.suppress(ImportError):
                ipykernel_comm = import_module("ipykernel.comm")

                for k, v in {"Comm": Comm, "CommManager": CommManager}.items():
                    setattr(ipykernel_comm, k, v)
