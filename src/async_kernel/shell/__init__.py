from __future__ import annotations

from typing import TYPE_CHECKING

from aiologic.meta import export_dynamic

from async_kernel.shell.base import BaseShell

export_dynamic(globals(), "IPShell", "async_kernel.shell.ipshell.IPShell")

if TYPE_CHECKING:
    from async_kernel.shell.ipshell import IPShell  # noqa: TC004

__all__ = ["BaseShell", "IPShell"]
