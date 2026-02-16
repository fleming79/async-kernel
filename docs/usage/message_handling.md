# Message handling

- When a message is received the `msg_handler` is called with:
    - 'job' (a dict of `msg`, `received_time` and `ident`)
    - The `channel`
    - `msg_type`
    - A function `send_reply`

- The `msg_handler`
    - determines the `subshell_id` and [run mode](#run-mode).
    - obtains the `handler` from the kernel with the same name as the `msg_type`.
    - determines the [run mode](#run-mode)
    - creates cached version of the `run_handler` with a unique version per:
        - The `handler`
        - `channel`
        - `subshell_id`
        - send_reply (constant or per-channel)
    - Obtains the caller associated with the channel and schedules execution of the cached handler

## Run mode

The run modes available are:

- `RunMode.direct` → [async_kernel.caller.Caller.call_direct][]
  Run the request directly in the scheduler.
- `RunMode.queue` → [async_kernel.caller.Caller.queue_call][]
  Run the request in a queue dedicated to the subshell, handler & channel.
- `RunMode.task` → [async_kernel.caller.Caller.call_soon][]
  Run the request in a separate task.
- `RunMode.thread` → [async_kernel.caller.Caller.to_thread][]
  Run the request in a separate worker thread.

These are the currently assigned run modes.

| SocketID                | shell  | control |
| ----------------------- | ------ | ------- |
| comm_close              | direct | direct  |
| comm_info_request       | direct | direct  |
| comm_msg                | queue  | queue   |
| comm_open               | direct | direct  |
| complete_request        | thread | thread  |
| create_subshell_request | None   | thread  |
| debug_request           | None   | queue   |
| delete_subshell_request | None   | thread  |
| execute_request         | queue  | queue   |
| history_request         | thread | thread  |
| inspect_request         | thread | thread  |
| interrupt_request       | direct | direct  |
| is_complete_request     | thread | thread  |
| kernel_info_request     | direct | direct  |
| list_subshell_request   | None   | direct  |
| shutdown_request        | None   | direct  |
