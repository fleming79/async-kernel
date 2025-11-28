import sys
from importlib.metadata import version
from typing import TYPE_CHECKING

import aiologic.meta

from async_kernel import utils
from async_kernel.caller import Caller
from async_kernel.pending import Pending

__version__ = version(distribution_name="async-kernel")

kernel_protocol_version = "5.4"
kernel_protocol_version_info = {
    "name": "python",
    "version": ".".join(map(str, sys.version_info[0:3])),
    "mimetype": "text/x-python",
    "codemirror_mode": {"name": "ipython", "version": 3},
    "pygments_lexer": "ipython3",
    "nbconvert_exporter": "python",
    "file_extension": ".py",
}

if TYPE_CHECKING:
    from async_kernel.kernel import Kernel  # noqa: F401

# Dynamic import
aiologic.meta.export_dynamic(globals(), "Kernel", ".kernel.Kernel")

__all__ = [
    "Caller",
    "Pending",
    "__version__",
    "kernel_protocol_version",
    "kernel_protocol_version_info",
    "utils",
]
if sys.platform != "emscripten":
    # Kernel needs zmq and threading. Both are not supported on Pyodide.
    __all__.append("Kernel")  # noqa: PYI056
