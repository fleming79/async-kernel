# From https://github.com/richardsheridan/trio-guest/blob/69a349c21563b641e333de02ee88ee8d5c1a3a52/trio_guest_qt5.py
#
# Modifications Copyright 2020 Richard J. Sheridan
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Modifications 2026 MIT License

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any, Literal

from aiologic.meta import import_from
from typing_extensions import override

from ._run import Host

if TYPE_CHECKING:
    from collections.abc import Callable

    from PySide6 import QtCore, QtWidgets  # noqa: TC004


def get_host(module: str = "PySide6") -> Host:
    """
    Run an async function with qt event loop using trio guest mode.

    args:
        module: https://matplotlib.org/stable/api/backend_qt_api.html#qt-bindings
        **kwargs: Ignored
    """
    if threading.current_thread() is not threading.main_thread():
        msg = "QT can only be run in main thread!"
        raise RuntimeError(msg)

    globals()["QtCore"] = import_from(module, "QtCore")
    globals()["QtWidgets"] = import_from(module, "QtWidgets")

    # class Reenter(QtCore.QObject):
    #     run = QtCore.Signal(object)
    #
    # This is substantially faster than using a signal... for some reason Qt
    # signal dispatch is really slow (and relies on events underneath anyway, so
    # this is strictly less work)
    REENTER_EVENT_TYPE = QtCore.QEvent.Type(QtCore.QEvent.registerEventType())

    class ReenterEvent(QtCore.QEvent):
        fn: Callable[[], Any]

    class Reenter(QtCore.QObject):
        @override
        def event(self, event: ReenterEvent) -> Literal[False]:  # pyright: ignore[reportIncompatibleMethodOverride]
            event.fn()
            return False

    class QtHost(Host):
        def __init__(self, app: QtWidgets.QApplication) -> None:
            self.app = app
            self.reenter = Reenter()
            # or if using Signal
            # self.reenter.run.connect(lambda fn: fn(), QtCore.Qt.QueuedConnection)
            # self.run_sync_soon_threadsafe = self.reenter.run.emit

        @override
        def run_sync_soon_threadsafe(self, fn) -> None:
            event = ReenterEvent(REENTER_EVENT_TYPE)
            event.fn = fn
            self.app.postEvent(self.reenter, event)

        @override
        def run_sync_soon_not_threadsafe(self, fn) -> None:
            event = ReenterEvent(REENTER_EVENT_TYPE)
            event.fn = fn
            self.app.postEvent(self.reenter, event)

        @override
        def done_callback(self, outcome) -> None:
            super().done_callback(outcome)
            self.app.quit()

        @override
        def mainloop(self) -> None:
            self.app.exec()
            return super().mainloop()

    app = QtWidgets.QApplication.instance()
    if app is None:
        app = QtWidgets.QApplication([])
        app.setQuitOnLastWindowClosed(False)  # prevent app sudden death
    return QtHost(app)  # pyright: ignore[reportArgumentType]
