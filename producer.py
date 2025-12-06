import socket
from common import send_json, recv_json
from config import BROKER_HOST, BROKER_PORT

def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((BROKER_HOST, BROKER_PORT))
    task_payload = {"type": "send_email", "to": "user@example.com", "body": "Hello from DistQueue"}
    send_json(s, {"type": "submit_task", "queue": "email", "payload": task_payload})
    resp = recv_json(s)
    print("Broker response:", resp)
    s.close()

if __name__ == "__main__":
    main()
