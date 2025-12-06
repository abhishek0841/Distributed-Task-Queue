import socket
import time
from common import send_json, recv_json
from config import BROKER_HOST, BROKER_PORT

def process_task(task):
    # dummy work
    print("Processing task:", task["task_id"], task["payload"])
    time.sleep(2)
    return True

def main():
    while True:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((BROKER_HOST, BROKER_PORT))
        send_json(s, {"type": "request_task", "queue": "email"})
        resp = recv_json(s)
        if resp["type"] == "task":
            success = process_task(resp)
            send_json(s, {"type": "task_done", "task_id": resp["task_id"], "success": success})
            ack = recv_json(s)
            print("Ack from broker:", ack)
        else:
            s.close()
            time.sleep(1)  # no tasks; backoff

if __name__ == "__main__":
    main()
