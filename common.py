import json

def send_json(sock, obj):
    data = json.dumps(obj).encode("utf-8") + b"\n"
    sock.sendall(data)

def recv_json(sock):
    """
    Receive exactly one JSON object delimited by a newline.
    Ignores empty / whitespace-only lines.
    Returns None if the connection is closed.
    """
    buf = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            # connection closed
            if not buf:
                return None
            # fall through to try parsing whatever is left
        buf += chunk
        # Process all complete lines in the buffer
        while b"\n" in buf:
            line, _, buf = buf.partition(b"\n")
            line = line.strip()
            if not line:
                # empty line, skip
                if not buf:
                    # need more data
                    break
                else:
                    continue
            try:
                return json.loads(line.decode("utf-8"))
            except json.JSONDecodeError:
                # malformed line, skip and continue reading
                # (you can print this for debugging if you want)
                # print("Bad JSON line:", line)
                if not buf:
                    break
                else:
                    continue
