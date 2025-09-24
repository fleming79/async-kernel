# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] - 2025-09-24

### <!-- 0 --> 🏗️ Breaking changes

- Simplify queue with breaking changes [#134](https://github.com/fleming79/async-kernel/pull/134)

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
