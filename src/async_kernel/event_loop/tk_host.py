# From https://github.com/richardsheridan/trio-guest/blob/69a349c21563b641e333de02ee88ee8d5c1a3a52/trio_guest_tkinter.py
# Copyright 2020 Richard J. Sheridan
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

import collections
import tkinter as tk
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from async_kernel.typing import Loop

from .run import Host

if TYPE_CHECKING:
    from collections.abc import Callable


class TkHost(Host):
    LOOP = Loop.tk
    MATPLOTLIB_GUIS = ("tk",)

    def __init__(self) -> None:
        root = tk.Tk()
        root.withdraw()
        self.root = root
        self._tk_func_name = root.register(self._tk_func)
        self._q = collections.deque()

    def _tk_func(self) -> None:
        self._q.popleft()()

    @override
    def run_sync_soon_threadsafe(self, fn: Callable[[], Any]) -> None:
        """
        Use Tcl "after" command to schedule a function call

        Based on `tkinter source comments <https://github.com/python/cpython/blob/a5d6aba318ead9cc756ba750a70da41f5def3f8f/Modules/_tkinter.c#L1472-L1555>`_
        the issuance of the tcl call to after itself is thread-safe since it is sent
        to the `appropriate thread <https://github.com/python/cpython/blob/a5d6aba318ead9cc756ba750a70da41f5def3f8f/Modules/_tkinter.c#L814-L824>`_ on line 1522.
        Tkapp_ThreadSend effectively uses "after 0" while putting the command in the
        event queue so the `"after idle after 0" <https://wiki.tcl-lang.org/page/after#096aeab6629eae8b244ae2eb2000869fbe377fa988d192db5cf63defd3d8c061>`_ incantation
        is unnecessary here.

        Compare to `tkthread <https://github.com/serwy/tkthread/blob/1f612e1dd46e770bd0d0bb64d7ecb6a0f04875a3/tkthread/__init__.py#L163>`_
        where definitely thread unsafe `eval <https://github.com/python/cpython/blob/a5d6aba318ead9cc756ba750a70da41f5def3f8f/Modules/_tkinter.c#L1567-L1585>`_
        is used to send thread safe signals between tcl interpreters.
        """
        # self.root.after_idle(lambda:self.root.after(0, func)) # does a fairly intensive wrapping to each func
        self._q.append(fn)
        self.root.call("after", "idle", self._tk_func_name)

    @override
    def run_sync_soon_not_threadsafe(self, fn) -> None:
        """
        Use Tcl "after" command to schedule a function call from the main thread

        If .call is called from the Tcl thread, the locking and sending are optimized away
        so it should be fast enough.

        The incantation `"after idle after 0" <https://wiki.tcl-lang.org/page/after#096aeab6629eae8b244ae2eb2000869fbe377fa988d192db5cf63defd3d8c061>`_ avoids blocking the normal event queue when
        faced with an unending stream of tasks, for example "while True: await trio.sleep(0)".
        """
        self._q.append(fn)
        self.root.call("after", "idle", "after", 0, self._tk_func_name)
        # Not sure if this is actually an optimization because Tcl parses this eval string fresh each time.
        # However it's definitely thread unsafe because the string is fed directly into the Tcl interpreter
        # from the current Python thread
        # self.root.eval(f'after idle after 0 {self._tk_func_name}')

    @override
    def done_callback(self, outcome) -> None:
        """
        End the Tk app.
        """
        super().done_callback(outcome)
        self.root.destroy()

    @override
    def mainloop(self) -> Any:
        self.start_guest()
        self.root.mainloop()
        return super().mainloop()
