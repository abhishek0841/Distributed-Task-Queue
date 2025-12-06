import socket
import threading
import queue
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
import json

from common import send_json, recv_json
from config import BROKER_HOST, BROKER_PORT, VISIBILITY_TIMEOUT_SEC, MAX_RETRIES

class MetricsHandler(BaseHTTPRequestHandler):
    broker_ref = None  # this will be set at runtime

    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return

        metrics = {}
        with self.broker_ref.lock:
            metrics["pending"] = sum(q.qsize() for q in self.broker_ref.queues.values())
            metrics["in_flight"] = sum(
                1 for t in self.broker_ref.tasks.values() if t["status"] == "in_flight"
            )
            metrics["dead_letter"] = sum(
                1 for t in self.broker_ref.tasks.values() if t["status"] == "dead_letter"
            )

        data = json.dumps(metrics).encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(data)

class Broker:
    def __init__(self):
        # queue_name -> queue.Queue of task_ids
        self.queues = {}
        # task_id -> task_metadata
        self.tasks = {}
        self.lock = threading.Lock()

    def submit_task(self, queue_name, payload):
        task_id = str(uuid.uuid4())
        task = {
            "task_id": task_id,
            "queue": queue_name,
            "payload": payload,
            "attempts": 0,
            "status": "queued",     # queued | in_flight | done | dead_letter
            "last_lease": None,
        }
        with self.lock:
            if queue_name not in self.queues:
                self.queues[queue_name] = queue.Queue()
            self.tasks[task_id] = task
            self.queues[queue_name].put(task_id)
        return task_id

    def lease_task(self, queue_name):
        with self.lock:
            q = self.queues.get(queue_name)
            if not q or q.empty():
                return None
            task_id = q.get()
            task = self.tasks.get(task_id)
            if not task:
                return None
            task["attempts"] += 1
            task["status"] = "in_flight"
            task["last_lease"] = time.time()
            return task

    def ack_task(self, task_id, success: bool):
        with self.lock:
            task = self.tasks.get(task_id)
            if not task:
                return
            if success:
                task["status"] = "done"
            else:
                # retry or dead-letter
                if task["attempts"] >= MAX_RETRIES:
                    task["status"] = "dead_letter"
                else:
                    task["status"] = "queued"
                    q = self.queues.setdefault(task["queue"], queue.Queue())
                    q.put(task_id)

    def reap_expired_leases(self):
        """Background thread: re-queue tasks whose visibility timeout expired."""
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
                                q = self.queues.setdefault(task["queue"], queue.Queue())
                                q.put(task["task_id"])
            time.sleep(5)


def handle_client(sock, addr, broker: "Broker"):
    try:
        while True:
            msg = recv_json(sock)
            if msg is None:
                # client closed connection or no valid message
                break

            mtype = msg.get("type")
            if mtype == "submit_task":
                tid = broker.submit_task(msg["queue"], msg["payload"])
                send_json(sock, {"type": "submit_ack", "task_id": tid})

            elif mtype == "request_task":
                task = broker.lease_task(msg["queue"])
                if task:
                    send_json(sock, {
                        "type": "task",
                        "task_id": task["task_id"],
                        "payload": task["payload"],
                        "attempts": task["attempts"],
                    })
                else:
                    send_json(sock, {"type": "no_task"})

            elif mtype == "task_done":
                broker.ack_task(msg["task_id"], msg["success"])
                send_json(sock, {"type": "ack_received"})

            else:
                # unknown message type – ignore or log
                # print("Unknown message type:", msg)
                pass

    except Exception as e:
        # Optional debug log:
        # print(f"Error handling client {addr}: {e}")
        pass
    finally:
        sock.close()


def main():
    broker = Broker()

    # attach broker to metrics handler
    MetricsHandler.broker_ref = broker

    # start metrics http server on port 8000
    metrics_server = HTTPServer(("127.0.0.1", 8000), MetricsHandler)
    threading.Thread(target=metrics_server.serve_forever, daemon=True).start()
    print("Metrics server running on http://127.0.0.1:8000/metrics")

    # Start background reaper thread
    threading.Thread(target=broker.reap_expired_leases, daemon=True).start()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Allow quick restart on same port
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((BROKER_HOST, BROKER_PORT))
    s.listen()
    print(f"Broker listening on {BROKER_HOST}:{BROKER_PORT}")

    while True:
        client_sock, addr = s.accept()
        t = threading.Thread(target=handle_client, args=(client_sock, addr, broker), daemon=True)
        t.start()


if __name__ == "__main__":
    main()
