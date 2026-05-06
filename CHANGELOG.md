# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.18.3] - 2026-05-06

### <!-- 1 --> 🚀 Features

- Include default argument in pack_json_bytes  and pack_json_str. [#456](https://github.com/fleming79/async-kernel/pull/456)

## [0.18.2] - 2026-05-03

### <!-- 2 --> 🐛 Fixes

- Fix Caller.call_later not sleeping correctly. [#454](https://github.com/fleming79/async-kernel/pull/454)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.18.2 [#455](https://github.com/fleming79/async-kernel/pull/455)

## [0.18.1] - 2026-04-26

### <!-- 2 --> 🐛 Fixes

- Refactor OutStream and fix utils.redirect_stderr. [#450](https://github.com/fleming79/async-kernel/pull/450)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.18.1 [#453](https://github.com/fleming79/async-kernel/pull/453)

- Check trio is available early when creating a Caller for a trio backend. [#452](https://github.com/fleming79/async-kernel/pull/452)

- Remove unnecessary line. [#451](https://github.com/fleming79/async-kernel/pull/451)

## [0.18.0] - 2026-04-25

### <!-- 0 --> 🏗️ Breaking changes

- Move kernel_info, help_links and debugger from the shell to the kernel or interface. [#444](https://github.com/fleming79/async-kernel/pull/444)

### <!-- 1 --> 🚀 Features

- Add utils.redirect_stdout and utils.redirect_stderr. [#442](https://github.com/fleming79/async-kernel/pull/442)

- Make system calls asynchronous and convert uv and pip magics to use it. [#441](https://github.com/fleming79/async-kernel/pull/441)

### <!-- 2 --> 🐛 Fixes

- Improve uv support and fix example kernel spec [#440](https://github.com/fleming79/async-kernel/pull/440)

### <!-- 5 --> 📝 Documentation

- Update Caller docstrings [#446](https://github.com/fleming79/async-kernel/pull/446)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.18.0 [#449](https://github.com/fleming79/async-kernel/pull/449)

- Refactor AsyncInteractiveShell.shell method [#448](https://github.com/fleming79/async-kernel/pull/448)

- Refactor Caller.as_completed. [#447](https://github.com/fleming79/async-kernel/pull/447)

- Change kernel_info back to a property. [#445](https://github.com/fleming79/async-kernel/pull/445)

- Change magics thread, asynio and trio back to coroutines that don't return results. [#443](https://github.com/fleming79/async-kernel/pull/443)

## [0.17.1] - 2026-04-23

### <!-- 0 --> 🏗️ Breaking changes

- Drop the tag 'suppress_error'. [#436](https://github.com/fleming79/async-kernel/pull/436)

- BaseKernelInterface refactoring [#434](https://github.com/fleming79/async-kernel/pull/434)

### <!-- 1 --> 🚀 Features

- Add asyncio and trio cell and line magic [#437](https://github.com/fleming79/async-kernel/pull/437)

- Support specifing the thread with cell magic and passing thread options [#435](https://github.com/fleming79/async-kernel/pull/435)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.17.1 [#439](https://github.com/fleming79/async-kernel/pull/439)

- Update pre-commit, uv.lock and basedpyright [#438](https://github.com/fleming79/async-kernel/pull/438)

## [0.17.0] - 2026-04-22

### <!-- 0 --> 🏗️ Breaking changes

- Transfer methods from Kernel to interface [#432](https://github.com/fleming79/async-kernel/pull/432)

- Improve kernelspec to simplify creating kernelspecs for virtual environments. [#431](https://github.com/fleming79/async-kernel/pull/431)

- Make AsyncInteractiveShell and AsyncInteractiveSubshell easier to subclass. [#430](https://github.com/fleming79/async-kernel/pull/430)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.17.0 [#433](https://github.com/fleming79/async-kernel/pull/433)

## [0.16.4] - 2026-04-15

### <!-- 2 --> 🐛 Fixes

- Fix PendingGroup mode 0 and add mode 3. [#428](https://github.com/fleming79/async-kernel/pull/428)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.16.4 [#429](https://github.com/fleming79/async-kernel/pull/429)

## [0.16.3] - 2026-04-12

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.16.3 [#427](https://github.com/fleming79/async-kernel/pull/427)

- Run comm_open and com_close message handlers in the shell thread. [#426](https://github.com/fleming79/async-kernel/pull/426)

## [0.16.2] - 2026-04-12

### <!-- 1 --> 🚀 Features

- Provide async line magic support [#424](https://github.com/fleming79/async-kernel/pull/424)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.16.2 [#425](https://github.com/fleming79/async-kernel/pull/425)

## [0.16.1] - 2026-04-08

### <!-- 1 --> 🚀 Features

- Add shield option to Pending.cancel_wait. [#421](https://github.com/fleming79/async-kernel/pull/421)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.16.1 [#423](https://github.com/fleming79/async-kernel/pull/423)

- Test against Python 3.15 [#422](https://github.com/fleming79/async-kernel/pull/422)

## [0.16.0] - 2026-04-06

### <!-- 0 --> 🏗️ Breaking changes

- Change KernelInterruptError to KernelInterrupt [#405](https://github.com/fleming79/async-kernel/pull/405)

- Pending no longer raises an InvalidStateError if it is already done [#400](https://github.com/fleming79/async-kernel/pull/400)

### <!-- 1 --> 🚀 Features

- Caller maintenance and make SingleAsyncQueue publicly available [#411](https://github.com/fleming79/async-kernel/pull/411)

- Add shield option to Pending.wait [#410](https://github.com/fleming79/async-kernel/pull/410)

- Improve HistoryManager handling [#407](https://github.com/fleming79/async-kernel/pull/407)

### <!-- 2 --> 🐛 Fixes

- Fix Fixed.create_instance bug [#395](https://github.com/fleming79/async-kernel/pull/395)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.16.0 [#420](https://github.com/fleming79/async-kernel/pull/420)

- Caller refactoring [#419](https://github.com/fleming79/async-kernel/pull/419)

- Convert Caller._get_task_factory to a function [#416](https://github.com/fleming79/async-kernel/pull/416)

- Move async_kernel.caller.SingleAsyncQueue to async_kernel.common.SingleAsyncQueue [#415](https://github.com/fleming79/async-kernel/pull/415)

- Add SingleAsyncQueue.appendleft. [#414](https://github.com/fleming79/async-kernel/pull/414)

- KernelInterrupt subclass from InterruptedError. [#413](https://github.com/fleming79/async-kernel/pull/413)

- Update uv.lock. [#412](https://github.com/fleming79/async-kernel/pull/412)

- Caller refactoring [#409](https://github.com/fleming79/async-kernel/pull/409)

- Caller maintenance [#408](https://github.com/fleming79/async-kernel/pull/408)

- Improve reliability of test_comm_open_msg_close [#406](https://github.com/fleming79/async-kernel/pull/406)

- Caller maintenance [#404](https://github.com/fleming79/async-kernel/pull/404)

- Improve Pending._set_done thread safety [#403](https://github.com/fleming79/async-kernel/pull/403)

- Pending._set_done refactor. [#401](https://github.com/fleming79/async-kernel/pull/401)

- Set session.check_pid to False  which is used for every comm message sent. (os.getpid is not cheap). [#399](https://github.com/fleming79/async-kernel/pull/399)

- Caller refactoring [#398](https://github.com/fleming79/async-kernel/pull/398)

- Refactor Pending [#397](https://github.com/fleming79/async-kernel/pull/397)

- Use THREAD_DUMMY_LOCK to save memory [#396](https://github.com/fleming79/async-kernel/pull/396)

- Bump codecov/codecov-action from 5 to 6 in the actions group [#392](https://github.com/fleming79/async-kernel/pull/392)

## [0.15.0] - 2026-03-30

### <!-- 0 --> 🏗️ Breaking changes

- Provide cancellation in Pending by default [#386](https://github.com/fleming79/async-kernel/pull/386)

- Use a standard dict for the user_ns (remove LastUpdatedDict) [#380](https://github.com/fleming79/async-kernel/pull/380)

### <!-- 1 --> 🚀 Features

- Replace aiologic.Lock with aiologic.lowlevel.create_thread_oncelock for improved performance [#389](https://github.com/fleming79/async-kernel/pull/389)

- Convert Pending._done_callbacks from deque to list [#383](https://github.com/fleming79/async-kernel/pull/383)

- Free-threading preliminary support. [#379](https://github.com/fleming79/async-kernel/pull/379)

### <!-- 5 --> 📝 Documentation

- Update docstrings [#384](https://github.com/fleming79/async-kernel/pull/384)

- Add thread_safety.md [#381](https://github.com/fleming79/async-kernel/pull/381)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.15.0 [#394](https://github.com/fleming79/async-kernel/pull/394)

- Change Pending.wait_sync signature to be the same as Pending.wait [#393](https://github.com/fleming79/async-kernel/pull/393)

- Tidy up [#391](https://github.com/fleming79/async-kernel/pull/391)

- Pending refactoring. [#390](https://github.com/fleming79/async-kernel/pull/390)

- Use aiologic.lowlevel.create_async_waiter instead of Event for better performance. [#388](https://github.com/fleming79/async-kernel/pull/388)

- Pending refactoring [#385](https://github.com/fleming79/async-kernel/pull/385)

- Caller.queue_call optimizations [#382](https://github.com/fleming79/async-kernel/pull/382)

- Add Kernel.run and refactor the interfaces. [#378](https://github.com/fleming79/async-kernel/pull/378)

- Use Kernel._get_run_mode for all message types. [#377](https://github.com/fleming79/async-kernel/pull/377)

- Add a VS Code launch config for to launch Jupyterlab [#376](https://github.com/fleming79/async-kernel/pull/376)

- Upgrade uv.lock and run gui tests using windows. [#375](https://github.com/fleming79/async-kernel/pull/375)

- Update uv.lock [#374](https://github.com/fleming79/async-kernel/pull/374)

## [0.14.0] - 2026-03-24

### <!-- 0 --> 🏗️ Breaking changes

- Rename loop to host and Loop to Hosts [#370](https://github.com/fleming79/async-kernel/pull/370)

### <!-- 5 --> 📝 Documentation

- Update event loop documentation. [#372](https://github.com/fleming79/async-kernel/pull/372)

- Update readme [#371](https://github.com/fleming79/async-kernel/pull/371)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.14.0 [#373](https://github.com/fleming79/async-kernel/pull/373)

## [0.13.3] - 2026-03-23

### <!-- 0 --> 🏗️ Breaking changes

- Housekeeping [#365](https://github.com/fleming79/async-kernel/pull/365)

- General maintenance [#361](https://github.com/fleming79/async-kernel/pull/361)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.13.3 [#369](https://github.com/fleming79/async-kernel/pull/369)

- Minor SingleConsumerAsyncQueue optimization. [#368](https://github.com/fleming79/async-kernel/pull/368)

- Fixtest_command_start_kernel_enable_matplotlib [#367](https://github.com/fleming79/async-kernel/pull/367)

- Substitute control thread for shell-hidden thread and limit subshell handler to execute_requests [#366](https://github.com/fleming79/async-kernel/pull/366)

- Refactor ZMQKernelInterface [#364](https://github.com/fleming79/async-kernel/pull/364)

- Simplify CommManager and Comm - the Kernel is now Fixed [#363](https://github.com/fleming79/async-kernel/pull/363)

- Access kernel from shell where it is available for AsyncDisplayPublisher and KernelMagics [#362](https://github.com/fleming79/async-kernel/pull/362)

## [0.13.2] - 2026-03-15

### <!-- 0 --> 🏗️ Breaking changes

- Simplify message handling [#357](https://github.com/fleming79/async-kernel/pull/357)

### <!-- 1 --> 🚀 Features

- Add more methods defined on the IPyKernels base Kernel. [#354](https://github.com/fleming79/async-kernel/pull/354)

### <!-- 5 --> 📝 Documentation

- Update documentation [#356](https://github.com/fleming79/async-kernel/pull/356)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.13.2 [#360](https://github.com/fleming79/async-kernel/pull/360)

- Prevent signal interrupt when the debugger is paused [#359](https://github.com/fleming79/async-kernel/pull/359)

- Pre-commit and uv.lock update [#358](https://github.com/fleming79/async-kernel/pull/358)

- Do not prevent message types on the shell channel. [#355](https://github.com/fleming79/async-kernel/pull/355)

- Bump asyncio_guest to 71st revision. [#353](https://github.com/fleming79/async-kernel/pull/353)

- Bump asyncio_guest to 69th rev (Better ProactorEventLoop experience) [#352](https://github.com/fleming79/async-kernel/pull/352)

## [0.13.1] - 2026-03-02

### <!-- 2 --> 🐛 Fixes

- Fix Caller.as_complete not returning for empty iterable. [#349](https://github.com/fleming79/async-kernel/pull/349)

### <!-- 5 --> 📝 Documentation

- Tidy up asyncshell.py. [#350](https://github.com/fleming79/async-kernel/pull/350)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.13.1 [#351](https://github.com/fleming79/async-kernel/pull/351)

- Bump the actions group with 2 updates [#348](https://github.com/fleming79/async-kernel/pull/348)

## [0.13.0] - 2026-03-01

### <!-- 1 --> 🚀 Features

- Revise concurrency modes with a  new RunMode - RunMode.thread_queue [#346](https://github.com/fleming79/async-kernel/pull/346)

- Add 'folder' as an option for get_kernel_dir [#344](https://github.com/fleming79/async-kernel/pull/344)

### <!-- 5 --> 📝 Documentation

- Consistently use lower case async kernel. [#345](https://github.com/fleming79/async-kernel/pull/345)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.13.0 [#347](https://github.com/fleming79/async-kernel/pull/347)

- Change msg_type from "display_data" to "execute_result" in finish_displayhook. [#343](https://github.com/fleming79/async-kernel/pull/343)

## [0.12.5] - 2026-02-25

### <!-- 0 --> 🏗️ Breaking changes

- Reduce complexity of Pending and PendingTracker and Caller [#339](https://github.com/fleming79/async-kernel/pull/339)

### <!-- 1 --> 🚀 Features

- Add mode option to PendingGroup [#338](https://github.com/fleming79/async-kernel/pull/338)

### <!-- 5 --> 📝 Documentation

- Documentation updates [#340](https://github.com/fleming79/async-kernel/pull/340)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.12.5 [#342](https://github.com/fleming79/async-kernel/pull/342)

- Maker Caller.checkpoint private (Caller._checkpoint). [#341](https://github.com/fleming79/async-kernel/pull/341)

## [0.12.4] - 2026-02-23

### <!-- 0 --> 🏗️ Breaking changes

- Switch from using thread ident to id of thread. Also keep a reference to the thread in the caller. [#330](https://github.com/fleming79/async-kernel/pull/330)

### <!-- 1 --> 🚀 Features

- Support run_guest_mode in Caller [#331](https://github.com/fleming79/async-kernel/pull/331)

### <!-- 5 --> 📝 Documentation

- Update README.md [#336](https://github.com/fleming79/async-kernel/pull/336)

- Documentation updates [#335](https://github.com/fleming79/async-kernel/pull/335)

- Compatibility fix for mkdocs-autorefs v1.4.4 [#333](https://github.com/fleming79/async-kernel/pull/333)

- Fix link to command line usage in readme [#324](https://github.com/fleming79/async-kernel/pull/324)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.12.4 [#337](https://github.com/fleming79/async-kernel/pull/337)

- Update the asyncio guest mode [#334](https://github.com/fleming79/async-kernel/pull/334)

- Pre-commit autoupdate and uv upgrade [#332](https://github.com/fleming79/async-kernel/pull/332)

- Tidy Caller.as_completed. [#329](https://github.com/fleming79/async-kernel/pull/329)

- Caller refactoring [#328](https://github.com/fleming79/async-kernel/pull/328)

- Pending.wait minor refactor. [#327](https://github.com/fleming79/async-kernel/pull/327)

- Refactor Caller.as_completed [#326](https://github.com/fleming79/async-kernel/pull/326)

- Fix spelling of subshell_id [#325](https://github.com/fleming79/async-kernel/pull/325)

## [0.12.3] - 2026-02-18

### <!-- 0 --> 🏗️ Breaking changes

- Fix PendingTracker for id leakage when opting out of tracking [#319](https://github.com/fleming79/async-kernel/pull/319)

### <!-- 1 --> 🚀 Features

- Added SingleConsumerAsyncQueue to caller.py [#322](https://github.com/fleming79/async-kernel/pull/322)

- Add support for running various event loops with the backend running as a guest [#320](https://github.com/fleming79/async-kernel/pull/320)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.12.3 [#323](https://github.com/fleming79/async-kernel/pull/323)

- Make Caller.checkpoint safer. [#321](https://github.com/fleming79/async-kernel/pull/321)

- Raise a PendingCancelled in a PendingGroup when cancelled via the cancel scope. [#318](https://github.com/fleming79/async-kernel/pull/318)

- Raise ExceptionGroup in PendingGroup when a pending fails. [#317](https://github.com/fleming79/async-kernel/pull/317)

- PendingTracker refactoring [#316](https://github.com/fleming79/async-kernel/pull/316)

## [0.12.2] - 2026-02-10

### <!-- 0 --> 🏗️ Breaking changes

- Change Pending trackers default from `PendingTracker`  to () meaning that tracking is opt in rather than opt out. [#314](https://github.com/fleming79/async-kernel/pull/314)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.12.2 [#315](https://github.com/fleming79/async-kernel/pull/315)

- Maintenance [#313](https://github.com/fleming79/async-kernel/pull/313)

## [0.12.1] - 2026-02-08

### <!-- 0 --> 🏗️ Breaking changes

- Dropped PendingCreateOptions in favour of trackers [#310](https://github.com/fleming79/async-kernel/pull/310)

- Improve PendingTracker, PendingManager and PendingGroup reliability [#308](https://github.com/fleming79/async-kernel/pull/308)

### <!-- 1 --> 🚀 Features

- Add Caller.queue_call_advanced and allow for tracking of queue_call associated pending. [#309](https://github.com/fleming79/async-kernel/pull/309)

- Improve Caller.wait to pre-check for pending and done. [#307](https://github.com/fleming79/async-kernel/pull/307)

### <!-- 2 --> 🐛 Fixes

- Fix Caller.queue_call not capturing the result of coroutines. [#306](https://github.com/fleming79/async-kernel/pull/306)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.12.1 [#312](https://github.com/fleming79/async-kernel/pull/312)

- Make pending group cancellation equivalent to Pending cancellation [#311](https://github.com/fleming79/async-kernel/pull/311)

## [0.12.0] - 2026-01-25

### <!-- 0 --> 🏗️ Breaking changes

- Drop the enum KernelName and no longer 'auto' select trio. [#303](https://github.com/fleming79/async-kernel/pull/303)

- Moved `kernel.transport` to` interface.transport [#301](https://github.com/fleming79/async-kernel/pull/301)

- Caller asyncio scheduling optimization and update readme [#297](https://github.com/fleming79/async-kernel/pull/297)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.12.0 [#305](https://github.com/fleming79/async-kernel/pull/305)

- Tweak ci to only run once typechecking passes. [#304](https://github.com/fleming79/async-kernel/pull/304)

- Chore - pre-commit autoupdate and uv.lock [#300](https://github.com/fleming79/async-kernel/pull/300)

- Expand kernel tests to run with uvloop/winloop where it is available [#299](https://github.com/fleming79/async-kernel/pull/299)

- Add a checkpoint that accepts backend as the argument. [#298](https://github.com/fleming79/async-kernel/pull/298)

- Remove jupyter_client as a build dependency. [#296](https://github.com/fleming79/async-kernel/pull/296)

- Make the AsyncInteractiveShell.kernel a trait [#295](https://github.com/fleming79/async-kernel/pull/295)

## [0.11.2] - 2026-01-18

### <!-- 1 --> 🚀 Features

- Implement iopub_welcome for JEP65 [#292](https://github.com/fleming79/async-kernel/pull/292)

- Provide a show_in_pager hook. [#291](https://github.com/fleming79/async-kernel/pull/291)

### <!-- 5 --> 📝 Documentation

- Update docs [#293](https://github.com/fleming79/async-kernel/pull/293)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.11.2 [#294](https://github.com/fleming79/async-kernel/pull/294)

## [0.11.1] - 2026-01-12

### <!-- 1 --> 🚀 Features

- Remove overrides of `run_cell`, `should_run_async` and `debug`, not testing, but assumed to be functional. [#289](https://github.com/fleming79/async-kernel/pull/289)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.11.1 [#290](https://github.com/fleming79/async-kernel/pull/290)

- Add kernel_protocol_version to kernelspec. [#288](https://github.com/fleming79/async-kernel/pull/288)

## [0.11.0] - 2026-01-04

### <!-- 0 --> 🏗️ Breaking changes

- Renamed SocketID to Channel and embed the channel inside the message and use channel in place of socket_id. [#286](https://github.com/fleming79/async-kernel/pull/286)

### <!-- 2 --> 🐛 Fixes

- Fix to_thread for pyodide. [#283](https://github.com/fleming79/async-kernel/pull/283)

- Fix Caller.as_completed sometimes does not yield the last result. [#282](https://github.com/fleming79/async-kernel/pull/282)

### <!-- 5 --> 📝 Documentation

- Update notebook examples [#285](https://github.com/fleming79/async-kernel/pull/285)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.11.0 [#287](https://github.com/fleming79/async-kernel/pull/287)

- Make interrupt tests more deterministic. [#284](https://github.com/fleming79/async-kernel/pull/284)

- Metadata and buffers [#281](https://github.com/fleming79/async-kernel/pull/281)

## [0.10.3] - 2026-01-02

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.10.3 [#280](https://github.com/fleming79/async-kernel/pull/280)

- Always set buffers in incoming messages in the callable interface. [#279](https://github.com/fleming79/async-kernel/pull/279)

## [0.10.2] - 2026-01-02

### <!-- 0 --> 🏗️ Breaking changes

- Added a callback style interface compatible with Jupyterlite [#277](https://github.com/fleming79/async-kernel/pull/277)

- Use ident instead of thread for caller mapping. [#276](https://github.com/fleming79/async-kernel/pull/276)

- Add an interface abstraction to the kernel with view to enable usage in pyodide. [#275](https://github.com/fleming79/async-kernel/pull/275)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.10.2 [#278](https://github.com/fleming79/async-kernel/pull/278)

- Bump the actions group with 2 updates [#274](https://github.com/fleming79/async-kernel/pull/274)

## [0.10.1] - 2025-12-14

### <!-- 0 --> 🏗️ Breaking changes

- Improve Caller.queue_call rewake reliability and revise Pending.set_result. [#272](https://github.com/fleming79/async-kernel/pull/272)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.10.1 [#273](https://github.com/fleming79/async-kernel/pull/273)

- Allow PendingManager.deactivate to be called multiple times. [#271](https://github.com/fleming79/async-kernel/pull/271)

## [0.10.0] - 2025-12-11

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.10.0 [#270](https://github.com/fleming79/async-kernel/pull/270)

- Maintenance [#269](https://github.com/fleming79/async-kernel/pull/269)

## [0.10.0-rc2] - 2025-12-10

### <!-- 5 --> 📝 Documentation

- Subshell docstrings [#267](https://github.com/fleming79/async-kernel/pull/267)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.10.0-rc2 [#268](https://github.com/fleming79/async-kernel/pull/268)

- Fix Subshell resetting main_shell namespace reset. [#266](https://github.com/fleming79/async-kernel/pull/266)

## [0.10.0-rc1] - 2025-12-09

### <!-- 0 --> 🏗️ Breaking changes

- Rename Pending.wait argument shield as protect. [#249](https://github.com/fleming79/async-kernel/pull/249)

- Drop MetadataKeys and rename execute_request_timeout to timeout and more tags. [#247](https://github.com/fleming79/async-kernel/pull/247)

- Refactoring with view to supporting pyodide in the kernel. [#244](https://github.com/fleming79/async-kernel/pull/244)

- Move RunMode.get_mode functionalty to Kernel.get_run_mode [#240](https://github.com/fleming79/async-kernel/pull/240)

### <!-- 1 --> 🚀 Features

- Add LastUpdatedDict and use it for the shell user_ns and user_global_ns. [#262](https://github.com/fleming79/async-kernel/pull/262)

- Use standard dictionary for Fixed instead of weakkeydict for faster access. [#258](https://github.com/fleming79/async-kernel/pull/258)

- Added PendingGroup and caller.create_pending_group. [#252](https://github.com/fleming79/async-kernel/pull/252)

- Added SubshellPendingManager. [#251](https://github.com/fleming79/async-kernel/pull/251)

- Caller.queue_call is now reset awaitable [#250](https://github.com/fleming79/async-kernel/pull/250)

- Added PendingTracker,  PendingManager and make Pending.set_result resettable. [#248](https://github.com/fleming79/async-kernel/pull/248)

- Update uv.lock and bump anyio min version to 4.12 with support for winloop. [#243](https://github.com/fleming79/async-kernel/pull/243)

- Add support for kernel subshells. [#238](https://github.com/fleming79/async-kernel/pull/238)

- Run mode header can now be either `# task` or `##task`. [#239](https://github.com/fleming79/async-kernel/pull/239)

- Bump aiologic min version to 0.16 and use its import features. [#234](https://github.com/fleming79/async-kernel/pull/234)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.10.0-rc1 [#265](https://github.com/fleming79/async-kernel/pull/265)

- Maintenance [#264](https://github.com/fleming79/async-kernel/pull/264)

- Use gc.collect at kernel shutdown. [#263](https://github.com/fleming79/async-kernel/pull/263)

- SubshellManager.create_subshell returns a subshell instead of the subshell_id. [#261](https://github.com/fleming79/async-kernel/pull/261)

- Modify tests involving weakref to work with pypy. [#260](https://github.com/fleming79/async-kernel/pull/260)

- Fix type hint for kernel.shell. [#259](https://github.com/fleming79/async-kernel/pull/259)

- Kernel maintenance [#257](https://github.com/fleming79/async-kernel/pull/257)

- Alternate kernel run modes. [#256](https://github.com/fleming79/async-kernel/pull/256)

- Use a tuple to pack direct call instead a functools partial. [#255](https://github.com/fleming79/async-kernel/pull/255)

- Do_complete_request bugfix. [#253](https://github.com/fleming79/async-kernel/pull/253)

- Make signature of AsyncInteractiveShell.inspect_request consistent with other methods. [#246](https://github.com/fleming79/async-kernel/pull/246)

- Move shell related requests to the shell. [#245](https://github.com/fleming79/async-kernel/pull/245)

- Refactor Caller for improved shutdown. [#242](https://github.com/fleming79/async-kernel/pull/242)

- Accept subshell_id from either the header or content. Content gets first option. [#241](https://github.com/fleming79/async-kernel/pull/241)

- Add checkpoints to Caller. [#237](https://github.com/fleming79/async-kernel/pull/237)

- Add py.typed [#236](https://github.com/fleming79/async-kernel/pull/236)

- Restore Kernel to the module namespace. [#235](https://github.com/fleming79/async-kernel/pull/235)

## [0.9.2] - 2025-11-27

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.9.2 [#233](https://github.com/fleming79/async-kernel/pull/233)

- Fix typing_extensions min version. [#232](https://github.com/fleming79/async-kernel/pull/232)

## [0.9.1] - 2025-11-27

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.9.1 [#231](https://github.com/fleming79/async-kernel/pull/231)

- Move 'kernel' optional dependencies into normal dependencies. [#230](https://github.com/fleming79/async-kernel/pull/230)

- Caller.start_sync now just uses asyncio.create_task instead of using an anyio token. [#229](https://github.com/fleming79/async-kernel/pull/229)

## [0.9.0] - 2025-11-25

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.9.0 [#228](https://github.com/fleming79/async-kernel/pull/228)

- Maintenance [#227](https://github.com/fleming79/async-kernel/pull/227)

- Compat layer isn't required. [#226](https://github.com/fleming79/async-kernel/pull/226)

## [0.9.0-rc.4] - 2025-11-25

### <!-- 0 --> 🏗️ Breaking changes

- Caller refactoring and breaking changes; renamed 'async-context' to 'manual' and removed Caller.to_thread_advanced. [#222](https://github.com/fleming79/async-kernel/pull/222)

- Make Caller() the preferred way to obtain a running caller. [#217](https://github.com/fleming79/async-kernel/pull/217)

- Kernel refactoring - moving code around for better readability. [#215](https://github.com/fleming79/async-kernel/pull/215)

### <!-- 1 --> 🚀 Features

- Towards making async_kernel.Callable usable on pyodide. [#223](https://github.com/fleming79/async-kernel/pull/223)

- Added time based idle worker cleanup. [#219](https://github.com/fleming79/async-kernel/pull/219)

- Caller.as_completed and Caller.wait can now wait for any awaitables. [#218](https://github.com/fleming79/async-kernel/pull/218)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.9.0-rc.4 [#225](https://github.com/fleming79/async-kernel/pull/225)

- Remove rudundant code in Caller.__new__. [#224](https://github.com/fleming79/async-kernel/pull/224)

- Bump actions/checkout from 5 to 6 in the actions group [#221](https://github.com/fleming79/async-kernel/pull/221)

- Prepare for release v0.9.0-rc.4 [#220](https://github.com/fleming79/async-kernel/pull/220)

- Test with free threading python [#216](https://github.com/fleming79/async-kernel/pull/216)

## [0.9.0-rc.3] - 2025-11-19

### <!-- 0 --> 🏗️ Breaking changes

- Caller.wait - renamed argument 'shield' to 'cancel_unfinished' and inverted the logic. [#213](https://github.com/fleming79/async-kernel/pull/213)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.9.0-rc.3 [#214](https://github.com/fleming79/async-kernel/pull/214)

## [0.9.0-rc.2] - 2025-11-19

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.9.0-rc.2 [#212](https://github.com/fleming79/async-kernel/pull/212)

- Name and hide Kernel.receive_msg_loop from debugpy. [#211](https://github.com/fleming79/async-kernel/pull/211)

- Tidy up Caller.get. [#210](https://github.com/fleming79/async-kernel/pull/210)

- Improved interrupts by adding a race to clear or re-reraise. [#209](https://github.com/fleming79/async-kernel/pull/209)

- Kernel - convert traits to Fixed [#208](https://github.com/fleming79/async-kernel/pull/208)

- Changed comm_manager to a Fixed property, the ipykernel patch is now only applied when the kernel is started. [#207](https://github.com/fleming79/async-kernel/pull/207)

- Bugfixes for 'Fixed' class [#206](https://github.com/fleming79/async-kernel/pull/206)

## [0.9.0-rc.1] - 2025-11-18

### <!-- 0 --> 🏗️ Breaking changes

- Caller - instance checks names of children rather than all instances. [#204](https://github.com/fleming79/async-kernel/pull/204)

- Caller restructuring adding new features and breaking changes plus added 'Fixed' class and renamed Future to Pending. [#197](https://github.com/fleming79/async-kernel/pull/197)

- Caller enhancements and breaking changes [#195](https://github.com/fleming79/async-kernel/pull/195)

- Remove unnecessary context copy and call from queue_call. [#194](https://github.com/fleming79/async-kernel/pull/194)

### <!-- 1 --> 🚀 Features

- Added Caller.zmq_context [#198](https://github.com/fleming79/async-kernel/pull/198)

- Use queue run mode instead of direct which is no slower, but probably 'safer'. [#193](https://github.com/fleming79/async-kernel/pull/193)

### <!-- 5 --> 📝 Documentation

- Docs [#203](https://github.com/fleming79/async-kernel/pull/203)

- Update readme. [#202](https://github.com/fleming79/async-kernel/pull/202)

- Docs and shuffle code inside the kernel [#200](https://github.com/fleming79/async-kernel/pull/200)

- Update mkdocs and mkdocstrings python - have released insiders features. [#192](https://github.com/fleming79/async-kernel/pull/192)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.9.0-rc.1 [#205](https://github.com/fleming79/async-kernel/pull/205)

- Add the trait Kernel.print_kernel_messages which when set to false prevents messages from being printed. [#201](https://github.com/fleming79/async-kernel/pull/201)

- General tidy and remove unrequired tests. [#199](https://github.com/fleming79/async-kernel/pull/199)

## [0.8.0] - 2025-11-12

### <!-- 0 --> 🏗️ Breaking changes

- Rename 'RunMode.blocking' to 'RunMode.direct'. [#190](https://github.com/fleming79/async-kernel/pull/190)

- Add __slots__ to Future. [#186](https://github.com/fleming79/async-kernel/pull/186)

- Drop Future.__init__ positional argument 'retain_metadata' [#185](https://github.com/fleming79/async-kernel/pull/185)

### <!-- 5 --> 📝 Documentation

- Maintenance and documentation. [#187](https://github.com/fleming79/async-kernel/pull/187)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.8.0 [#191](https://github.com/fleming79/async-kernel/pull/191)

- Prepare for release v0.7.2 [#189](https://github.com/fleming79/async-kernel/pull/189)

- General maintenance [#188](https://github.com/fleming79/async-kernel/pull/188)

## [0.7.1] - 2025-11-11

### <!-- 1 --> 🚀 Features

- Added Kernel.schedule_job, exposed Kernel.caller [#181](https://github.com/fleming79/async-kernel/pull/181)

### <!-- 5 --> 📝 Documentation

- Improve docstrings: [#182](https://github.com/fleming79/async-kernel/pull/182)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.7.1 [#184](https://github.com/fleming79/async-kernel/pull/184)

- Make all Threads daemon and add daemon option to Caller.start_new. [#183](https://github.com/fleming79/async-kernel/pull/183)

## [0.7.0] - 2025-11-10

### <!-- 0 --> 🏗️ Breaking changes

- Drop 'run_mode' key from Job dict. [#174](https://github.com/fleming79/async-kernel/pull/174)

- Remove usage of KernelConcurrencyMode. It was functional but unnecessary. [#173](https://github.com/fleming79/async-kernel/pull/173)

- Run shell and control socket loops in threads without event loops. [#172](https://github.com/fleming79/async-kernel/pull/172)

### <!-- 1 --> 🚀 Features

- Use BinarySemaphore instead of Lock for best performance in send_reply. [#178](https://github.com/fleming79/async-kernel/pull/178)

- Use a lock in send_reply. [#175](https://github.com/fleming79/async-kernel/pull/175)

### <!-- 5 --> 📝 Documentation

- Use BinarySemaphore instead of Lock for best performance in send_reply and update readme. [#179](https://github.com/fleming79/async-kernel/pull/179)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.7.0 [#180](https://github.com/fleming79/async-kernel/pull/180)

- Switch from mdformat to prettier for formatting markdown [#177](https://github.com/fleming79/async-kernel/pull/177)

- Improve typehints in tests. [#176](https://github.com/fleming79/async-kernel/pull/176)

- Added Kernel.run and permit the kernel to run outside the main thread [#171](https://github.com/fleming79/async-kernel/pull/171)

## [0.7.0-rc.2] - 2025-11-07

### <!-- 1 --> 🚀 Features

- Use low-level async primatives in caller module. [#169](https://github.com/fleming79/async-kernel/pull/169)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.7.0-rc.2 [#170](https://github.com/fleming79/async-kernel/pull/170)

## [0.7.0-rc.1] - 2025-11-04

### <!-- 0 --> 🏗️ Breaking changes

- Use aiologic for thread-safe Event and Lock. [#164](https://github.com/fleming79/async-kernel/pull/164)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.7.0-rc.1 [#167](https://github.com/fleming79/async-kernel/pull/167)

## [0.6.3] - 2025-11-04

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.6.3 [#166](https://github.com/fleming79/async-kernel/pull/166)

- Kernel.execute_request unnecessarily sent a thread call to a new thread. [#165](https://github.com/fleming79/async-kernel/pull/165)

- Update pre-commit [#163](https://github.com/fleming79/async-kernel/pull/163)

- Use a dependency floor in requirements and upgrade lock file. [#162](https://github.com/fleming79/async-kernel/pull/162)

- Make AsyncInteractiveShell.enable_gui raise NotImplementedError for unsupported guis. [#161](https://github.com/fleming79/async-kernel/pull/161)

## [0.6.2] - 2025-10-29

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.6.2 [#160](https://github.com/fleming79/async-kernel/pull/160)

- Bump the actions group with 2 updates [#158](https://github.com/fleming79/async-kernel/pull/158)

- Don't reraise caught cancelled error in Caller_wrap_call. [#159](https://github.com/fleming79/async-kernel/pull/159)

- Ensure debugInfo is fullly populated. [#157](https://github.com/fleming79/async-kernel/pull/157)

## [0.6.1] - 2025-10-17

### <!-- 1 --> 🚀 Features

- Added Kernel.get_parent. [#155](https://github.com/fleming79/async-kernel/pull/155)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.6.1 [#156](https://github.com/fleming79/async-kernel/pull/156)

- Bump astral-sh/setup-uv from 6 to 7 in the actions group [#154](https://github.com/fleming79/async-kernel/pull/154)

## [0.6.0] - 2025-09-30

### <!-- 0 --> 🏗️ Breaking changes

- Remove 'name' argument from get_instance (it can be provided as a kwarg. [#152](https://github.com/fleming79/async-kernel/pull/152)

- Rename Caller.to_thread_by_name to Caller.to_thread_advanced change the first argument from a string or None to a dict. [#151](https://github.com/fleming79/async-kernel/pull/151)

### <!-- 1 --> 🚀 Features

- Add hooks to AsyncDisplayPublisher [#150](https://github.com/fleming79/async-kernel/pull/150)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.6.0 [#153](https://github.com/fleming79/async-kernel/pull/153)

- Better handling of Keyboard Interrupt. [#149](https://github.com/fleming79/async-kernel/pull/149)

## [0.5.4] - 2025-09-28

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.5.4 [#148](https://github.com/fleming79/async-kernel/pull/148)

- Add functools.wraps decorator to kernel._wrap_handler to make it easier to identify which function it is wrapping. [#147](https://github.com/fleming79/async-kernel/pull/147)

- Minimize calls to 'expensive' thread.Event methods [#146](https://github.com/fleming79/async-kernel/pull/146)

## [0.5.3] - 2025-09-27

### <!-- 5 --> 📝 Documentation

- Various documentation improvements. [#144](https://github.com/fleming79/async-kernel/pull/144)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.5.3 [#145](https://github.com/fleming79/async-kernel/pull/145)

- Tidy up Caller queues and  remove kernel.CancelledError. [#143](https://github.com/fleming79/async-kernel/pull/143)

- Refactored ReentrantAsyncLock and AsyncLock with a new method 'base'. [#142](https://github.com/fleming79/async-kernel/pull/142)

## [0.5.2] - 2025-09-26

### <!-- 2 --> 🐛 Fixes

- Fix debugger [#140](https://github.com/fleming79/async-kernel/pull/140)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.5.2 [#141](https://github.com/fleming79/async-kernel/pull/141)

- Refactor Kernel and Subclass Caller from anyio.AsyncContextManagerMixin [#139](https://github.com/fleming79/async-kernel/pull/139)

## [0.5.1] - 2025-09-25

### <!-- 1 --> 🚀 Features

- Take advantage of current_token in utils.wait_thread_event. [#136](https://github.com/fleming79/async-kernel/pull/136)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.5.1 [#138](https://github.com/fleming79/async-kernel/pull/138)

- Reinstate test_debugger for windows. [#137](https://github.com/fleming79/async-kernel/pull/137)

## [0.5.0] - 2025-09-24

### <!-- 0 --> 🏗️ Breaking changes

- Simplify queue with breaking changes [#134](https://github.com/fleming79/async-kernel/pull/134)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.5.0 [#135](https://github.com/fleming79/async-kernel/pull/135)

## [0.4.0] - 2025-09-23

### <!-- 0 --> 🏗️ Breaking changes

- Revise message handling for comm_msg [#129](https://github.com/fleming79/async-kernel/pull/129)

- Improve Calller.get_instance to start a caller for the main thread if there isn't one running. [#127](https://github.com/fleming79/async-kernel/pull/127)

### <!-- 1 --> 🚀 Features

- Make Caller.queue_call and Caller.queue_call_no_wait thread safe [#131](https://github.com/fleming79/async-kernel/pull/131)

- Add  Caller.get_runner. [#126](https://github.com/fleming79/async-kernel/pull/126)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.4.0 [#133](https://github.com/fleming79/async-kernel/pull/133)

- Maintenance [#132](https://github.com/fleming79/async-kernel/pull/132)

- Put _send_reply back inside run_handler. [#130](https://github.com/fleming79/async-kernel/pull/130)

- Prevent memory leaks in caller scheduled futures [#128](https://github.com/fleming79/async-kernel/pull/128)

- Housekeeping [#125](https://github.com/fleming79/async-kernel/pull/125)

## [0.3.0] - 2025-09-14

### <!-- 0 --> 🏗️ Breaking changes

- Caller.queue_call - divide into queue_get_sender, queue_call and queue_call_no_wait. [#123](https://github.com/fleming79/async-kernel/pull/123)

- Stricter handling in Caller class. [#122](https://github.com/fleming79/async-kernel/pull/122)

- Add AsyncEvent  class. [#118](https://github.com/fleming79/async-kernel/pull/118)

### <!-- 1 --> 🚀 Features

- Store Caller.call_later function details in the futures  metadata [#119](https://github.com/fleming79/async-kernel/pull/119)

- Add metadata to Future. [#116](https://github.com/fleming79/async-kernel/pull/116)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.3.0 [#124](https://github.com/fleming79/async-kernel/pull/124)

- AsyncEvent maintenance - make more robust [#120](https://github.com/fleming79/async-kernel/pull/120)

- Switch from pytest-retry to pytest-rerun failures. [#117](https://github.com/fleming79/async-kernel/pull/117)

- Refactor Caller to speed up initialization of Future by removing the creation of the threading event. [#115](https://github.com/fleming79/async-kernel/pull/115)

## [0.2.1] - 2025-09-10

### <!-- 0 --> 🏗️ Breaking changes

- Maintenance [#105](https://github.com/fleming79/async-kernel/pull/105)

### <!-- 1 --> 🚀 Features

- Divide Lock into AsyncLock and ReentrantAsyncLock [#113](https://github.com/fleming79/async-kernel/pull/113)

- Improve Lock class [#112](https://github.com/fleming79/async-kernel/pull/112)

- Add a context based Lock [#111](https://github.com/fleming79/async-kernel/pull/111)

- Add classmethod  Caller.wait [#106](https://github.com/fleming79/async-kernel/pull/106)

- Add 'shield' option to Caller.as_completed. [#104](https://github.com/fleming79/async-kernel/pull/104)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.2.1 [#114](https://github.com/fleming79/async-kernel/pull/114)

- Bump actions/setup-python from 5 to 6 in the actions group [#110](https://github.com/fleming79/async-kernel/pull/110)

- Maintenance - Caller refactoring [#109](https://github.com/fleming79/async-kernel/pull/109)

- Drop WaitType for Literals directly in Caller.wait. [#108](https://github.com/fleming79/async-kernel/pull/108)

- Change Caller._queue_map to a WeakKeyDictionary. [#107](https://github.com/fleming79/async-kernel/pull/107)

- Refactor Caller.wait to avoid catching  exceptions. [#103](https://github.com/fleming79/async-kernel/pull/103)

## [0.2.0] - 2025-09-06

### <!-- 0 --> 🏗️ Breaking changes

- Rename Caller.call_no_context to Caller.call_direct. [#100](https://github.com/fleming79/async-kernel/pull/100)

- Future - breaking changes- better compatibility of Future.result [#96](https://github.com/fleming79/async-kernel/pull/96)

### <!-- 1 --> 🚀 Features

- Add the classmethod Caller.current_future. [#99](https://github.com/fleming79/async-kernel/pull/99)

- Add timeout, shield and result optional arguments to Future wait and wait_sync methods: [#97](https://github.com/fleming79/async-kernel/pull/97)

- Add  optional argument 'msg' to Future.cancel method. [#95](https://github.com/fleming79/async-kernel/pull/95)

- Support weakref on the Future class. [#94](https://github.com/fleming79/async-kernel/pull/94)

### <!-- 5 --> 📝 Documentation

- Documentation maintenance. [#101](https://github.com/fleming79/async-kernel/pull/101)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.2.0 [#102](https://github.com/fleming79/async-kernel/pull/102)

- Result should raise cancelled error, but was raising and InvalidStateError. [#98](https://github.com/fleming79/async-kernel/pull/98)

## [0.1.4] - 2025-09-03

### <!-- 0 --> 🏗️ Breaking changes

- Optionally store a string representation of a kernel factory inside the kernel spec. [#92](https://github.com/fleming79/async-kernel/pull/92)

- Use capital 'V' instead of 'v'  for version flag in command_line. [#88](https://github.com/fleming79/async-kernel/pull/88)

### <!-- 5 --> 📝 Documentation

- Fix for publish-docs.yml not  setting the version info correctly. [#90](https://github.com/fleming79/async-kernel/pull/90)

- Include changelog in 'dev' version of docs. [#89](https://github.com/fleming79/async-kernel/pull/89)

- Development documentation updates and fixes for publish-docs.yml. [#87](https://github.com/fleming79/async-kernel/pull/87)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.1.4 [#93](https://github.com/fleming79/async-kernel/pull/93)

- Ensure there is only one kernel instance including subclases. [#91](https://github.com/fleming79/async-kernel/pull/91)

## [0.1.3] - 2025-09-02

### <!-- 1 --> 🚀 Features

- Add version option to command line. [#82](https://github.com/fleming79/async-kernel/pull/82)

### <!-- 2 --> 🐛 Fixes

- Fix bug setting version for mike. [#80](https://github.com/fleming79/async-kernel/pull/80)

### <!-- 5 --> 📝 Documentation

- Update documentation [#84](https://github.com/fleming79/async-kernel/pull/84)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.1.3 [#86](https://github.com/fleming79/async-kernel/pull/86)

- Minor import changes. [#85](https://github.com/fleming79/async-kernel/pull/85)

- Change base class of Kernel from ConnectionFileMixin to HasTraits [#83](https://github.com/fleming79/async-kernel/pull/83)

- Overwrite subclass properties that should not be available. [#81](https://github.com/fleming79/async-kernel/pull/81)

- CI checks for python 3.14 [#63](https://github.com/fleming79/async-kernel/pull/63)

## [0.1.2] - 2025-08-31

### <!-- 0 --> 🏗️ Breaking changes

- Breaking changes to kernel initialisation and launching [#78](https://github.com/fleming79/async-kernel/pull/78)

- Enhancement -  Make kernel async enterable. [#77](https://github.com/fleming79/async-kernel/pull/77)

### <!-- 5 --> 📝 Documentation

- Fix alias for latest docs and limit release versions. [#75](https://github.com/fleming79/async-kernel/pull/75)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.1.2 [#79](https://github.com/fleming79/async-kernel/pull/79)

- CI and pre-commit maintenance [#76](https://github.com/fleming79/async-kernel/pull/76)

## [0.1.1] - 2025-08-28

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.1.1 [#74](https://github.com/fleming79/async-kernel/pull/74)

- Bugfixes - fix installing without trio and installing a kernelspec [#73](https://github.com/fleming79/async-kernel/pull/73)

## [0.1.0] - 2025-08-28

### <!-- 0 --> 🏗️ Breaking changes

- Caller.queue_call add argument send_nowait  and convert to sync that optionally returns an awaitable. [#71](https://github.com/fleming79/async-kernel/pull/71)

### <!-- 1 --> 🚀 Features

- Add anyio_backend_options and use uvloop by default [#70](https://github.com/fleming79/async-kernel/pull/70)

### <!-- 5 --> 📝 Documentation

- Use mike for documentation versioning. [#67](https://github.com/fleming79/async-kernel/pull/67)

- Update docs, readme and project description. [#66](https://github.com/fleming79/async-kernel/pull/66)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.1.0 [#72](https://github.com/fleming79/async-kernel/pull/72)

- Drop matplotlib dependency. [#69](https://github.com/fleming79/async-kernel/pull/69)

## [0.1.0-rc3] - 2025-08-26

### <!-- 1 --> 🚀 Features

- Add more classifers and code coverage [#64](https://github.com/fleming79/async-kernel/pull/64)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.1.0-rc3 [#65](https://github.com/fleming79/async-kernel/pull/65)

- Add workflow_run event because the release is not triggered if  the release is created by another workflow. [#62](https://github.com/fleming79/async-kernel/pull/62)

## [0.1.0-rc2] - 2025-08-26

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.1.0-rc2 [#61](https://github.com/fleming79/async-kernel/pull/61)

## [0.1.0-rc1] - 2025-08-26

### <!-- 5 --> 📝 Documentation

- Update licensing and contribution notes [#27](https://github.com/fleming79/async-kernel/pull/27)

### <!-- 6 --> 🌀 Miscellaneous

- Prepare for release v0.1.0-rc1 [#60](https://github.com/fleming79/async-kernel/pull/60)

- Merge pull request #56 from fleming79/release/v0.1.0-rc1 [#56](https://github.com/fleming79/async-kernel/pull/56)

- Revise new release [#55](https://github.com/fleming79/async-kernel/pull/55)

- New release workflow in one step with publish option. [#51](https://github.com/fleming79/async-kernel/pull/51)

- Improve release workflow, update documentation and license info. [#29](https://github.com/fleming79/async-kernel/pull/29)

- Maintenance [#26](https://github.com/fleming79/async-kernel/pull/26)

## [0.1.0-rc0] - 2025-08-24

### <!-- 1 --> 🚀 Features

- First release [#18](https://github.com/fleming79/async-kernel/pull/18)

- Switch to vcs for versioning. [#2](https://github.com/fleming79/async-kernel/pull/2)

### <!-- 2 --> 🐛 Fixes

- Use no-local-version in pyproject.toml instead. [#5](https://github.com/fleming79/async-kernel/pull/5)

- Use no-local-version on ci. [#4](https://github.com/fleming79/async-kernel/pull/4)

### <!-- 5 --> 📝 Documentation

- Revise workflow to work with tags that start with 'v'. No longer sets the tag when writing the changelog. [#16](https://github.com/fleming79/async-kernel/pull/16)

- Switch to python installer to run git cliff. [#14](https://github.com/fleming79/async-kernel/pull/14)

- Revise changelog template. [#12](https://github.com/fleming79/async-kernel/pull/12)

- Do changelog as PR instead of push to main. [#8](https://github.com/fleming79/async-kernel/pull/8)

- Git cliff [#7](https://github.com/fleming79/async-kernel/pull/7)

- Fix mkdocs publishing [#6](https://github.com/fleming79/async-kernel/pull/6)

### <!-- 6 --> 🌀 Miscellaneous

- Bugfix [#25](https://github.com/fleming79/async-kernel/pull/25)

- Update changelog [#24](https://github.com/fleming79/async-kernel/pull/24)

- Update changelog [#22](https://github.com/fleming79/async-kernel/pull/22)

- Release workflow changes [#21](https://github.com/fleming79/async-kernel/pull/21)

- Update release workflow to use a template that appends output from git-cliff [#17](https://github.com/fleming79/async-kernel/pull/17)

- Bump the actions group across 1 directory with 2 updates [#3](https://github.com/fleming79/async-kernel/pull/3)

[0.18.3]: https://github.com/fleming79/async-kernel/compare/v0.18.2..v0.18.3
[0.18.2]: https://github.com/fleming79/async-kernel/compare/v0.18.1..v0.18.2
[0.18.1]: https://github.com/fleming79/async-kernel/compare/v0.18.0..v0.18.1
[0.18.0]: https://github.com/fleming79/async-kernel/compare/v0.17.1..v0.18.0
[0.17.1]: https://github.com/fleming79/async-kernel/compare/v0.17.0..v0.17.1
[0.17.0]: https://github.com/fleming79/async-kernel/compare/v0.16.4..v0.17.0
[0.16.4]: https://github.com/fleming79/async-kernel/compare/v0.16.3..v0.16.4
[0.16.3]: https://github.com/fleming79/async-kernel/compare/v0.16.2..v0.16.3
[0.16.2]: https://github.com/fleming79/async-kernel/compare/v0.16.1..v0.16.2
[0.16.1]: https://github.com/fleming79/async-kernel/compare/v0.16.0..v0.16.1
[0.16.0]: https://github.com/fleming79/async-kernel/compare/v0.15.0..v0.16.0
[0.15.0]: https://github.com/fleming79/async-kernel/compare/v0.14.0..v0.15.0
[0.14.0]: https://github.com/fleming79/async-kernel/compare/v0.13.3..v0.14.0
[0.13.3]: https://github.com/fleming79/async-kernel/compare/v0.13.2..v0.13.3
[0.13.2]: https://github.com/fleming79/async-kernel/compare/v0.13.1..v0.13.2
[0.13.1]: https://github.com/fleming79/async-kernel/compare/v0.13.0..v0.13.1
[0.13.0]: https://github.com/fleming79/async-kernel/compare/v0.12.5..v0.13.0
[0.12.5]: https://github.com/fleming79/async-kernel/compare/v0.12.4..v0.12.5
[0.12.4]: https://github.com/fleming79/async-kernel/compare/v0.12.3..v0.12.4
[0.12.3]: https://github.com/fleming79/async-kernel/compare/v0.12.2..v0.12.3
[0.12.2]: https://github.com/fleming79/async-kernel/compare/v0.12.1..v0.12.2
[0.12.1]: https://github.com/fleming79/async-kernel/compare/v0.12.0..v0.12.1
[0.12.0]: https://github.com/fleming79/async-kernel/compare/v0.11.2..v0.12.0
[0.11.2]: https://github.com/fleming79/async-kernel/compare/v0.11.1..v0.11.2
[0.11.1]: https://github.com/fleming79/async-kernel/compare/v0.11.0..v0.11.1
[0.11.0]: https://github.com/fleming79/async-kernel/compare/v0.10.3..v0.11.0
[0.10.3]: https://github.com/fleming79/async-kernel/compare/v0.10.2..v0.10.3
[0.10.2]: https://github.com/fleming79/async-kernel/compare/v0.10.1..v0.10.2
[0.10.1]: https://github.com/fleming79/async-kernel/compare/v0.10.0..v0.10.1
[0.10.0]: https://github.com/fleming79/async-kernel/compare/v0.10.0-rc2..v0.10.0
[0.10.0-rc2]: https://github.com/fleming79/async-kernel/compare/v0.10.0-rc1..v0.10.0-rc2
[0.10.0-rc1]: https://github.com/fleming79/async-kernel/compare/v0.9.2..v0.10.0-rc1
[0.9.2]: https://github.com/fleming79/async-kernel/compare/v0.9.1..v0.9.2
[0.9.1]: https://github.com/fleming79/async-kernel/compare/v0.9.0..v0.9.1
[0.9.0]: https://github.com/fleming79/async-kernel/compare/v0.9.0-rc.4..v0.9.0
[0.9.0-rc.4]: https://github.com/fleming79/async-kernel/compare/v0.9.0-rc.3..v0.9.0-rc.4
[0.9.0-rc.3]: https://github.com/fleming79/async-kernel/compare/v0.9.0-rc.2..v0.9.0-rc.3
[0.9.0-rc.2]: https://github.com/fleming79/async-kernel/compare/v0.9.0-rc.1..v0.9.0-rc.2
[0.9.0-rc.1]: https://github.com/fleming79/async-kernel/compare/v0.8.0..v0.9.0-rc.1
[0.8.0]: https://github.com/fleming79/async-kernel/compare/v0.7.1..v0.8.0
[0.7.1]: https://github.com/fleming79/async-kernel/compare/v0.7.0..v0.7.1
[0.7.0]: https://github.com/fleming79/async-kernel/compare/v0.7.0-rc.2..v0.7.0
[0.7.0-rc.2]: https://github.com/fleming79/async-kernel/compare/v0.7.0-rc.1..v0.7.0-rc.2
[0.7.0-rc.1]: https://github.com/fleming79/async-kernel/compare/v0.6.3..v0.7.0-rc.1
[0.6.3]: https://github.com/fleming79/async-kernel/compare/v0.6.2..v0.6.3
[0.6.2]: https://github.com/fleming79/async-kernel/compare/v0.6.1..v0.6.2
[0.6.1]: https://github.com/fleming79/async-kernel/compare/v0.6.0..v0.6.1
[0.6.0]: https://github.com/fleming79/async-kernel/compare/v0.5.4..v0.6.0
[0.5.4]: https://github.com/fleming79/async-kernel/compare/v0.5.3..v0.5.4
[0.5.3]: https://github.com/fleming79/async-kernel/compare/v0.5.2..v0.5.3
[0.5.2]: https://github.com/fleming79/async-kernel/compare/v0.5.1..v0.5.2
[0.5.1]: https://github.com/fleming79/async-kernel/compare/v0.5.0..v0.5.1
[0.5.0]: https://github.com/fleming79/async-kernel/compare/v0.4.0..v0.5.0
[0.4.0]: https://github.com/fleming79/async-kernel/compare/v0.3.0..v0.4.0
[0.3.0]: https://github.com/fleming79/async-kernel/compare/v0.2.1..v0.3.0
[0.2.1]: https://github.com/fleming79/async-kernel/compare/v0.2.0..v0.2.1
[0.2.0]: https://github.com/fleming79/async-kernel/compare/v0.1.4..v0.2.0
[0.1.4]: https://github.com/fleming79/async-kernel/compare/v0.1.3..v0.1.4
[0.1.3]: https://github.com/fleming79/async-kernel/compare/v0.1.2..v0.1.3
[0.1.2]: https://github.com/fleming79/async-kernel/compare/v0.1.1..v0.1.2
[0.1.1]: https://github.com/fleming79/async-kernel/compare/v0.1.0..v0.1.1
[0.1.0]: https://github.com/fleming79/async-kernel/compare/v0.1.0-rc3..v0.1.0
[0.1.0-rc3]: https://github.com/fleming79/async-kernel/compare/v0.1.0-rc2..v0.1.0-rc3
[0.1.0-rc2]: https://github.com/fleming79/async-kernel/compare/v0.1.0-rc1..v0.1.0-rc2
[0.1.0-rc1]: https://github.com/fleming79/async-kernel/compare/v0.1.0-rc0..v0.1.0-rc1

<!-- generated by git-cliff -->
