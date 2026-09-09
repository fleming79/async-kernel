---
title: Modules
description: API reference for async-kernel.
# icon: material/
# subtitle: A sub title
---

# Reference

The reference section provides documentation for each module in async-kernel.

## Interface

The interface is a singleton that acts as a single point of truth. It provides `Traitlets`
style configuration, starts the kernel and hosts connections.

The diagram below illustrates the relationship between various objects.

- Arrows represent messaging.
- Thick lines represent the current process.
- Serialized messages and illustrated by a dotted line.

```mermaid
flowchart TD
Interface(Interface)
LocalConnection1(Local connection 1)
LocalConnection2(Local connection 2)
ZMQConnection(ZMQ connection)
ZMQClient1(ZMQ client 1)
ZMQClient2(ZMQ client 2)
LocalClient1(Local client 1)
LocalClient2(Local client 2)
MainShell(Main Shell)
Subshell1(Subshell 1)
Subshell2(Subshell 2)

Interface === Kernel
Interface === ZMQConnection
Interface === LocalConnection1
Interface === LocalConnection2

Kernel <==> MainShell
Kernel <==> Subshell1
Kernel <==> Subshell2
Kernel <==> LocalConnection1
Kernel <==> LocalConnection2

subgraph Local client 1
LocalConnection1 <==> LocalClient1
end
subgraph Local client 2
LocalConnection2 <==> LocalClient2
end

ZMQConnection <==> Kernel
ZMQConnection <-. zmq socket .-> ZMQClient1
ZMQConnection <-. zmq socket .-> ZMQClient2
```

## Highlights

- [Interface][async_kernel.interface.Interface] - The kernel interface.
- [Kernel][async_kernel.kernel.Kernel] - The kernel.
- [Caller][async_kernel.caller.Caller] - A flexible interface for scheduling work across threads and asynchronous backends.
- [command_line][async_kernel.command.command_line] - The command line interface.
