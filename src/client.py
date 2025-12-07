import socket
import threading
import json
import time

BOOTSTRAP_HOST = "127.0.0.1"
BOOTSTRAP_PORT = 1234

# === CONNECT TO BOOTSTRAP SERVER ===

bs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
bs.connect((BOOTSTRAP_HOST, BOOTSTRAP_PORT))

bs.send(b"READY")  # Notify bootstrap server

def recv_line(conn, buf):
    """Read from connection (socket) until a newline byte is found, parse and return the line as decoded string
    and return any remaining bytes.

    Args:
        conn: blocking object with recv(bytes) -> bytes
        buf: bytes of previously received unprocessed data

    Raises:
        ConnectionError: if conn.recv() returns b"" indicating connection closed.

    Returns:
        (line_str, rest_bytes): line_str is the decoded bytes before the newline, 
                rest_bytes are bytes after the newline to be stored for next reads.
    """
    while b"\n" not in buf:
        data = conn.recv(4096)
        if not data:
            raise ConnectionError("socket closed")
        buf += data

    line, sep, rest = buf.partition(b"\n")
    return line.decode(), rest

def send_ndjson(conn, obj):
    """
    Serializes Python object as JSON, appends a newline and sends it.

    Args:
        conn: socket with sendall(bytes)
        obj: JSON Python object
    
    Notes:
    Uses newline-delimited JSON (NDJSON) message JSON + "\n". This blocks until all bytes are sent.
    """
    conn.sendall((json.dumps(obj) + "\n").encode())

# Receive assigned ID
bs_buf = b""
line, bs_buf = recv_line(bs, bs_buf)
obj = json.loads(line)
my_id = obj["your_id"]
print(f"[CLIENT] My ID = {my_id}")

# Data structures
peers = {}         # peer_id -> (ip, port)
connections = {}   # peer_id -> socket
leader_id = None
# with one receive buffer per connection we can handle each peer independently 
recv_buffers = {} # conn -> bytes

# === LISTEN FOR INCOMING PEER CONNECTIONS ===

listen_port = 50000 + my_id
listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("0.0.0.0", listen_port))
listener.listen()

print(f"[CLIENT] Listening for peers on port {listen_port}")

def accept_peers():
    """
    Accepts incoming peer connections and spawns handlers for them.
    """
    while True:
        conn, addr = listener.accept()
        recv_buffers[conn] = b""
        threading.Thread(
            target=handle_peer_connection, 
            args=(conn,),
            daemon=True
        ).start()

def handle_peer_connection(conn):
    """
    Handles a new incoming peer connection.
    Reads peer ID and stores connection.
    Adds receive buffer per connection.
    """
    try:
        line, rest = recv_line(conn, recv_buffers.get(conn, b""))
    except ConnectionError:
        return
    
    recv_buffers[conn] = rest
    msg = json.loads(line)
    pid = msg["id"]

    connections[pid] = conn
    print(f"[P2P] Incoming connection from peer {pid}")

threading.Thread(target=accept_peers, daemon=True).start()


# === CONNECT OUTGOING TO PEERS ===

def connect_to_peer(pid, ip, port):
    """
    Connects to another peer if not already connected.
    Sends own ID upon connecting.
    """
    if pid in connections:
        return

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((ip, port))
        recv_buffers[s] = b""
        send_ndjson(s, {"id": my_id})
        connections[pid] = s
        print(f"[P2P] Connected to peer {pid}")
    except:
        pass


# === BULLY ELECTION ALGORITHM ===

def run_bully():
    """
    Runs the Bully Election Algorithm:
    - Sends ELECTION messages to all peers with higher IDs
    - If no OK is received after timeout, becomes leader
    - Otherwise waits for a LEADER broadcast
    """
    global leader_id
    print("[BULLY] Starting election.")

    higher = [pid for pid in peers if pid > my_id]

    if not higher:
        # No higher peers → become leader
        leader_id = my_id
        broadcast({"type": "LEADER", "leader": my_id})
        print("[BULLY] I am the leader!")
        return

    # Notify higher-ID peers
    for pid in higher:
        try:
            send_ndjson(connections[pid], {"type": "ELECTION"})
        except:
            pass

    # Wait for OK responses
    time.sleep(2)

    # If nobody responded, become leader
    if leader_id is None:
        leader_id = my_id
        broadcast({"type": "LEADER", "leader": my_id})

def broadcast(obj):
    """
    Sends a message to all connected peers.
    """
    for pid, conn in connections.items():
        try:
            send_ndjson(conn, obj)
        except:
            pass


# === RECEIVE MESSAGES FROM PEERS ===

def listen_to_peers():
    """
    Continuously listens for incoming messages from all connected peers.

    Behavior:
    - Iterates over global connections (mapping peer_id -> socket).
    - Uses recv_buffers (mapping socket -> bytes) to combine partial reads.
    - Temporarily sets the socket timeout to 0.01 to avoid blocking a long time on recv().
    - Calls conn.recv(4096) and if a timeout occurs treats it as no data received
    - appends any received bytes to the per-connection buffer.
    - extracts complete lines (delimiter is "\n") from the buffer and 
    decodes each line, parses JSON and calls handle_message for each parsed message.
    - Stores any leftover partial bytes back into recv_buffers[conn] for the next iteration.
    - Sleeps 0.1s after processing all connection and repeats indefinitely
    """
    while True:
        for pid, conn in list(connections.items()):
            try:
                buf = recv_buffers.get(conn, b"")
                # read bytes without blocking too long
                conn.settimeout(0.01)
                try:
                    data = conn.recv(4096)
                except socket.timeout:
                    data = b""
                finally:
                    conn.settimeout(None)
                if data:
                    buf += data 
                    # process full lines
                    while b"\n"  in buf:
                        line, sep, buf = buf.partition(b"\n")
                        msg = json.loads(line.decode())
                        handle_message(pid, msg)
                    recv_buffers[conn] = buf
            except:
                pass
        time.sleep(0.1)

def handle_message(pid, msg):
    """
    Processes incoming peer messages:
    - ELECTION → respond OK if our ID is higher
    - LEADER → update leader_id
    """
    global leader_id

    if msg["type"] == "ELECTION":
        if my_id > pid:
            # Send OK to lower-ID peer
            send_ndjson(connections[pid], {"type": "OK", "from": my_id})
    
    elif msg["type"] == "LEADER":
        leader_id = msg["leader"]
        print(f"[BULLY] New leader elected: {leader_id}")

threading.Thread(target=listen_to_peers, daemon=True).start()


# === MAIN LOOP: HANDLE UPDATED PEER LIST ===

print("[CLIENT] Waiting for peer list from bootstrap...")

while True:
    try:
        while b"\n" not in bs_buf:
            data = bs.recv(4096)
            if not data:
                raise ConnectionError("bootstrap closed")
            bs_buf += data

        while b"\n" in bs_buf:
            line, sep, bs_buf = bs_buf.partition(b"\n")
            parsed = json.loads(line.decode())
            new_list = parsed["peers"]

            # Update peer list
            for pid, (ip, port) in new_list:
                peers[pid] = (ip, 50000 + pid)
                if pid != my_id:
                    connect_to_peer(pid, "127.0.0.1", 50000 + pid)

            # Run initial election
            if leader_id is None:
                time.sleep(1)
                run_bully()

            # Re-elect if a stronger peer joins
            elif any(pid > leader_id for pid in peers):
                print("[BULLY] A stronger peer joined, re-running election.")
                leader_id = None
                time.sleep(0.5)
                run_bully()
    except ConnectionError:
        break
