import socket
import threading
import json
import time

# === GLOBAL P2P STATE ===

my_id = None  # set by init_p2p

peers = {}          # peer_id -> (ip, port)
connections = {}    # peer_id -> socket
recv_buffers = {}   # socket -> bytes (partial data buffer)
leader_id = None    # current leader

ok_received = threading.Event()  # set when we receive OK to our ELECTION
last_seen = {}                   # pid -> last time we saw any message from that peer

HEARTBEAT_INTERVAL = 1.0   # seconds between heartbeats from leader
LEADER_TIMEOUT = 30.0       # if no messages from leader in this many seconds → suspect failure

_listener = None

# callbacks provided by client
_on_became_leader = None     # () -> None
_on_new_leader = None        # (leader_id: int, sender: int) -> None
_on_game_event = None        # (event_dict: dict) -> None


# === NDJSON UTILITIES (also used by client for bootstrap) ===

def recv_line(conn, buf: bytes):
    """
    Read from connection until a newline byte is found.
    Return (decoded_line, remaining_bytes).
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
    Uses newline-delimited JSON (NDJSON): JSON + "\n".
    """
    conn.sendall((json.dumps(obj) + "\n").encode())


# === INITIALIZATION ===

def init_p2p(local_id: int, on_became_leader, on_new_leader, on_game_event):
    """
    Initialize P2P layer for this client.
    - Sets my_id
    - Stores callbacks
    - Creates listener socket and starts accept_peers thread
    """
    global my_id, _listener, _on_became_leader, _on_new_leader, _on_game_event

    my_id = local_id
    _on_became_leader = on_became_leader
    _on_new_leader = on_new_leader
    _on_game_event = on_game_event

    listen_port = 50000 + my_id
    _listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    _listener.bind(("0.0.0.0", listen_port))
    _listener.listen()

    print(f"[CLIENT] Listening for peers on port {listen_port}")

    threading.Thread(target=_accept_peers, daemon=True).start()


# === PUBLIC HELPERS ===

def get_player_ids():
    """
    Return list of all known peer IDs plus ourselves.
    """
    return sorted(set(list(peers.keys()) + [my_id]))


def broadcast(obj):
    """
    Sends a JSON object to all connected peers.
    Also delivers game events locally via _on_game_event.
    """
    # Send to peers over the network
    for pid, conn in list(connections.items()):
        try:
            send_ndjson(conn, obj)
        except Exception:
            pass

    # Also handle our own game events locally
    if "event_name" in obj and _on_game_event:
        _on_game_event(obj)



def connect_to_peer(pid: int, ip: str, port: int):
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
    except Exception:
        # For now, silently ignore failures (peer may not be up yet)
        pass


def update_peers_from_bootstrap(new_list):
    """
    Called by client when bootstrap server sends updated peer list.
    - Updates peers dict
    - Connects to new peers
    - Triggers Bully election if needed
    """
    global leader_id

    for pid, (ip, port) in new_list:
        peers[pid] = (ip, 50000 + pid)  # still assume 50000+pid locally
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


def start_background_threads():
    """
    Start the P2P background threads:
    - listen_to_peers
    - heartbeat_monitor
    - leader_heartbeat
    """
    threading.Thread(target=_listen_to_peers, daemon=True).start()
    threading.Thread(target=_heartbeat_monitor, daemon=True).start()
    threading.Thread(target=_leader_heartbeat, daemon=True).start()


# === INTERNAL: ACCEPT INCOMING PEERS ===

def _accept_peers():
    """
    Accept incoming TCP connections from other peers.
    """
    while True:
        conn, addr = _listener.accept()
        recv_buffers[conn] = b""
        threading.Thread(
            target=_handle_peer_connection,
            args=(conn,),
            daemon=True
        ).start()


def _handle_peer_connection(conn: socket.socket):
    """
    First message from incoming peer is their ID.
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

    # If we already know the leader, tell the new peer
    if leader_id is not None:
        send_ndjson(conn, {"type": "LEADER", "leader": leader_id})


# === BULLY ELECTION ALGORITHM ===

def run_bully():
    """
    Runs the Bully Election Algorithm:
    - Sends ELECTION messages to all peers with higher IDs
    - If no OK is received after timeout, becomes leader
    - Otherwise waits for a LEADER broadcast (with timeout)
    """
    global leader_id
    print("[BULLY] Starting election.")

    higher = [pid for pid in peers if pid > my_id]
    ok_received.clear()

    # If no higher-ID peers → I am the leader
    if not higher:
        leader_id = my_id
        broadcast({"type": "LEADER", "leader": my_id})
        print("[BULLY] I am the leader! (no higher peers)")
        if _on_became_leader:
            _on_became_leader()
        return

    # Notify higher-ID peers
    for pid in higher:
        if pid in connections:
            try:
                send_ndjson(connections[pid], {"type": "ELECTION"})
            except Exception:
                pass

    # Wait for OK responses
    ok_happened = ok_received.wait(timeout=2)

    if not ok_happened:
        # No OK → assume we are the highest active peer
        leader_id = my_id
        broadcast({"type": "LEADER", "leader": my_id})
        print("[BULLY] I am the leader! (no OK received)")
        if _on_became_leader:
            _on_became_leader()
        return
    else:
        print("[BULLY] OK received, waiting for LEADER broadcast")
        start = time.time()
        while leader_id is None:
            time.sleep(0.1)
            if time.time() - start > 3:
                print("[BULLY] LEADER broadcast timeout → restarting election")
                # Restart election from scratch
                run_bully()
                return


# === MESSAGE LOOP ===

def _listen_to_peers():
    """
    Continuously listens for incoming messages from all connected peers.
    """
    global leader_id

    while True:
        for pid, conn in list(connections.items()):
            try:
                buf = recv_buffers.get(conn, b"")
                conn.settimeout(0.01)
                try:
                    data = conn.recv(4096)
                except socket.timeout:
                    data = b""
                except (ConnectionResetError, BrokenPipeError):
                    raise ConnectionError
                finally:
                    conn.settimeout(None)

                if data:
                    buf += data
                    # process complete lines
                    while b"\n" in buf:
                        line, sep, buf = buf.partition(b"\n")
                        msg = json.loads(line.decode())
                        # record last seen time for this peer
                        last_seen[pid] = time.time()
                        _handle_message(pid, msg)
                    recv_buffers[conn] = buf

                # If connection is closed
                if data == b"" and conn.fileno() == -1:
                    raise ConnectionError

            except ConnectionError:
                print(f"[P2P] Lost connection to peer {pid}")

                try:
                    conn.close()
                except Exception:
                    pass

                connections.pop(pid, None)
                recv_buffers.pop(conn, None)
                last_seen.pop(pid, None)

                # If the lost peer was the leader, start new election
                if leader_id == pid:
                    print("[BULLY] Leader disconnected → starting new election")
                    leader_id = None
                    threading.Thread(target=run_bully, daemon=True).start()

        time.sleep(0.1)


def _handle_message(pid, msg):
    """
    Processes incoming peer messages:
    - Control messages (have 'type' field)
    - Game events (have 'event_name' field)
    """
    if "type" in msg:
        _handle_bully_message(pid, msg)
    elif "event_name" in msg:
        if _on_game_event:
            _on_game_event(msg)


# === HEARTBEAT THREADS ===

def _heartbeat_monitor():
    """
    Followers monitor leader liveness based on last_seen timestamps.
    If the leader is silent for too long, they trigger a new election.
    """
    global leader_id
    while True:
        # Only followers (leader_id != my_id) should suspect leader failure
        if leader_id is not None and leader_id != my_id:
            if leader_id in last_seen:
                if time.time() - last_seen[leader_id] > LEADER_TIMEOUT:
                    print("[BULLY] Leader timeout → starting new election")
                    leader_id = None
                    threading.Thread(target=run_bully, daemon=True).start()
        time.sleep(2)


def _leader_heartbeat():
    """
    If we are the leader, periodically broadcast heartbeat messages so
    followers know we are still alive.
    """
    while True:
        if leader_id == my_id:
            broadcast({"type": "HEARTBEAT", "from": my_id})
        time.sleep(HEARTBEAT_INTERVAL)


# === BULLY CONTROL MESSAGE HANDLING ===

def _handle_bully_message(pid, msg):
    """
    Processes Bully control messages:
    - ELECTION → respond OK if our ID is higher
    - LEADER   → update leader_id
    - OK       → mark that some higher peer is alive
    - HEARTBEAT → liveness updates are handled in _listen_to_peers via last_seen
    """
    global leader_id

    mtype = msg.get("type")

    if mtype == "ELECTION":
        if my_id > pid:
            # Send OK to lower-ID peer
            if pid in connections:
                send_ndjson(connections[pid], {"type": "OK", "from": my_id})

    elif mtype == "LEADER":
        leader_id = msg["leader"]
        print(f"[BULLY] New leader elected: {leader_id}")

        if _on_new_leader:
            _on_new_leader(leader_id, pid)

    elif mtype == "OK":
        ok_received.set()

    elif mtype == "HEARTBEAT":
        # Nothing special needed: _listen_to_peers already updated last_seen[pid]
        pass




def get_player_ids():
    """
    Return list of all known peer IDs plus ourselves.
    """
    return sorted(set(list(peers.keys()) + [my_id]))


def get_local_id():
    """
    Return this node's ID.
    """
    return my_id


def get_leader_id():
    """
    Return current leader ID (or None if no leader elected yet).
    """
    return leader_id
