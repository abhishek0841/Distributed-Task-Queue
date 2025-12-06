import socket
import threading
import queue
import time
import uuid
from common import send_json, recv_json
from config import BROKER_HOST, BROKER_PORT, VISIBILITY_TIMEOUT_SEC, MAX_RETRIES

class Broker:
    def __init__(self):
        self.queues = {}  # queue_name -> queue.Queue of task_ids
        self.tasks = {}   # task_id -> task_metadata
        self.lock = threading.Lock()

    def submit_task(self, queue_name, payload):
        task_id = str(uuid.uuid4())
        task = {
            "task_id": task_id,
            "queue": queue_name,
            "payload": payload,
            "attempts": 0,
            "status": "queued",
            "last_lease": None,
        }
        with self.lock:
            self.tasks[task_id] = task
            self.queues.setdefault(queue_name, queue.Queue()).put(task_id)
        return task_id

    def lease_task(self, queue_name):
        with self.lock:
            q = self.queues.get(queue_name)
            if not q or q.empty():
                return None
            task_id = q.get()
            task = self.tasks[task_id]
            task["attempts"] += 1
            task["status"] = "in_flight"
            task["last_lease"] = time.time()
            return task

    def ack_task(self, task_id, success):
        with self.lock:
            task = self.tasks.get(task_id)
            if not task:
                return
            if success:
                task["status"] = "done"
            else:
                if task["attempts"] >= MAX_RETRIES:
                    task["status"] = "dead_letter"
                else:
                    task["status"] = "queued"
                    self.queues[task["queue"]].put(task_id)

    def reap_expired_leases(self):
        while True:
            now = time.time()
            with self.lock:
                for task in self.tasks.values():
                    if task["status"] == "in_flight" and task["last_lease"]:
                        if now - task["last_lease"] > VISIBILITY_TIMEOUT_SEC:
                            if task["attempts"] >= MAX_RETRIES:
                                task["status"] = "dead_letter"
                            else:
                                task["status"] = "queued"
                                self.queues[task["queue"]].put(task["task_id"])
            time.sleep(5)

def handle_client(sock, addr, broker: Broker):
    try:
        while True:
            msg = recv_json(sock)
            if msg is None:
                break
            mtype = msg.get("type")
            if mtype == "submit_task":
                tid = broker.submit_task(msg["queue"], msg["payload"])
                send_json(sock, {"type": "submit_ack", "task_id": tid})
            elif mtype == "request_task":
                task = broker.lease_task(msg["queue"])
                if task:
                    send_json(sock, {"type": "task", **task})
                else:
                    send_json(sock, {"type": "no_task"})
            elif mtype == "task_done":
                broker.ack_task(msg["task_id"], msg["success"])
                send_json(sock, {"type": "ack_received"})
    finally:
        sock.close()

def main():
    broker = Broker()
    threading.Thread(target=broker.reap_expired_leases, daemon=True).start()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((BROKER_HOST, BROKER_PORT))
    s.listen()
    print(f"Broker listening on {BROKER_HOST}:{BROKER_PORT}")

    while True:
        client_sock, addr = s.accept()
        t = threading.Thread(target=handle_client, args=(client_sock, addr, broker), daemon=True)
        t.start()

if __name__ == "__main__":
    main()
