import socket
import time
import argparse
from common import send_json, recv_json
from config import BROKER_HOST, BROKER_PORT
from random import random

def process_task(task):
    print(f"[Worker] Processing task: {task['task_id']}  Payload: {task['payload']}")
    time.sleep(2)

    # 80% success, 20% simulated failure (to test retries + DLQ)
    success = random.random() > 0.2
    print(f"[Worker] Task {task['task_id']} success={success}")
    return success

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", default="default", help="Queue name to listen on")
    args = parser.parse_args()

    queue = args.queue
    print(f"[Worker] Started. Listening on queue: {queue}")

    while True:
        # connect to broker
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((BROKER_HOST, BROKER_PORT))

        # request a task
        send_json(s, {"type": "request_task", "queue": queue})
        resp = recv_json(s)

        # no task available
        if resp["type"] == "no_task":
            s.close()
            time.sleep(1)
            continue

        # got a task
        if resp["type"] == "task":
            print(f"[Worker] Received task {resp['task_id']} (attempt {resp['attempts']})")

            success = process_task(resp)

            # send ack back
            send_json(s, {
                "type": "task_done",
                "task_id": resp["task_id"],
                "success": success
            })

            ack = recv_json(s)
            print(f"[Worker] Ack from broker: {ack}")

        s.close()


if __name__ == "__main__":
    main()
