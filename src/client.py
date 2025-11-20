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

# Receive assigned ID
my_id = json.loads(bs.recv(4096).decode())["your_id"]
print(f"[CLIENT] My ID = {my_id}")

# Data structures
peers = {}         # peer_id -> (ip, port)
connections = {}   # peer_id -> socket
leader_id = None


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
        threading.Thread(
            target=handle_peer_connection, 
            args=(conn,),
            daemon=True
        ).start()

def handle_peer_connection(conn):
    """
    Handles a new incoming peer connection.
    Reads peer ID and stores connection.
    """
    data = conn.recv(4096).decode()
    msg = json.loads(data)
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
        s.send(json.dumps({"id": my_id}).encode())
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
            connections[pid].send(json.dumps({"type": "ELECTION"}).encode())
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
    msg = json.dumps(obj).encode()

    for pid, conn in connections.items():
        try:
            conn.send(msg)
        except:
            pass


# === RECEIVE MESSAGES FROM PEERS ===

def listen_to_peers():
    """
    Continuously listens for incoming messages from all connected peers.
    """
    while True:
        for pid, conn in list(connections.items()):
            try:
                data = conn.recv(4096)
                if not data:
                    continue
                msg = json.loads(data.decode())
                handle_message(pid, msg)
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
            connections[pid].send(json.dumps({"type": "OK"}).encode())
            run_bully()

    elif msg["type"] == "LEADER":
        leader_id = msg["leader"]
        print(f"[BULLY] New leader elected: {leader_id}")

threading.Thread(target=listen_to_peers, daemon=True).start()


# === MAIN LOOP: HANDLE UPDATED PEER LIST ===

print("[CLIENT] Waiting for peer list from bootstrap...")

while True:
    peer_packet = bs.recv(4096)
    if not peer_packet:
        break

    parsed = json.loads(peer_packet.decode())
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
