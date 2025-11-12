import sys
from importlib.metadata import version

from async_kernel import utils
from async_kernel.caller import Caller, Future
from async_kernel.kernel import Kernel

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


__all__ = [
    "Caller",
    "Future",
    "Kernel",
    "__version__",
    "kernel_protocol_version",
    "kernel_protocol_version_info",
    "utils",
]
