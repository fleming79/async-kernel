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

from async_kernel.typing import Loop

from ._run import Host

if TYPE_CHECKING:
    from collections.abc import Callable

    from PySide6 import QtCore, QtWidgets  # noqa: TC004


class QtHost(Host):
    LOOP = Loop.qt
    MATPLOTLIB_GUIS = ("qt",)

    def __init__(self, module: str = "PySide6") -> None:
        if threading.current_thread() is not threading.main_thread():
            msg = "QT can only be run in main thread!"
            raise RuntimeError(msg)

        globals()["QtCore"] = import_from(module, "QtCore")
        globals()["QtWidgets"] = import_from(module, "QtWidgets")

        REENTER_EVENT_TYPE = QtCore.QEvent.Type(QtCore.QEvent.registerEventType())

        class ReenterEvent(QtCore.QEvent):
            fn: Callable[[], Any]

        class Reenter(QtCore.QObject):
            @override
            def event(self, event: ReenterEvent) -> Literal[False]:  # pyright: ignore[reportIncompatibleMethodOverride]
                event.fn()
                return False

        reenter = Reenter()

        if (app := QtWidgets.QApplication.instance()) is None:
            app = QtWidgets.QApplication([])
            app.setQuitOnLastWindowClosed(False)  # prevent app sudden death

        def run_soon_threadsafe(fn):
            event = ReenterEvent(REENTER_EVENT_TYPE)
            event.fn = fn
            app.postEvent(reenter, event)

        self.run_sync_soon_threadsafe = run_soon_threadsafe
        self.run_sync_soon_not_threadsafe = run_soon_threadsafe
        self.app = app

    @override
    def done_callback(self, outcome) -> None:
        super().done_callback(outcome)
        self.app.quit()

    @override
    def mainloop(self) -> None:
        self.start_guest()
        self.app.exec()
        return super().mainloop()
