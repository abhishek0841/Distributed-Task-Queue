# Distributed-Task-Queue - A Minimal Distributed Task Queue

DistQueue is a lightweight distributed task queue implemented from scratch in Python using TCP sockets.

It consists of:
- a central **broker** that manages queues and task metadata,
- multiple **workers** that pull and execute tasks,
- one or more **producers** that submit jobs to be processed asynchronously.

The system supports:
- at-least-once delivery,
- visibility timeouts,
- retries,
- and a dead-letter queue for permanently failing tasks.

## Features

- **Central broker** managing named queues (`email`, `reports`, etc.).
- **Task submission API** over a simple JSON-over-TCP protocol.
- **Workers** that poll for work, execute tasks, and send acknowledgements.
- **Retry logic** with configurable `MAX_RETRIES`.
- **Visibility timeout** so tasks are re-queued if workers crash or hang.
- **Dead-letter queue** for tasks that repeatedly fail.
- Designed to run on a single machine (for learning) but conceptually matches real-world distributed task systems.

## Architecture

- Producers connect to the broker and send `submit_task` messages.
- Workers connect to the broker and send `request_task` messages.
- The broker leases tasks to workers, tracks in-flight tasks, and re-queues them if not acknowledged within the visibility timeout.
- A background reaper thread scans for expired leases and handles retries / dead-lettering.

## How to Run

1. Start the broker:

```bash
python broker.py

2. Start one or more workers:

```bash
python worker.py
python worker.py  # in another terminal

3. Submit tasks via the producer:

```bash
python producer.py

You should see workers picking up tasks and the broker logging activity.

Future Improvements

Persistent queues backed by disk instead of in-memory.

HTTP/REST gateway for submitting tasks.

Metrics endpoint (pending tasks, in-flight tasks, dead-letter counts).

Priority queues and per-queue configuration.
