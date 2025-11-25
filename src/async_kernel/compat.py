from __future__ import annotations

from typing import TYPE_CHECKING

import wrapt

# Lazy import zmq to make library installable on pyodide.
zmq = wrapt.lazy_import("zmq")

if TYPE_CHECKING:
    import zmq  # noqa: TC004

__all__ = ["zmq"]
