import socket
import threading
import json
import time

from events import make_event, EVENT_PLAYER_LEAVE

# === GLOBAL P2P STATE ===

my_id = None

peers = {}          # peer_id -> (ip, port)
connections = {}    # peer_id -> socket
recv_buffers = {}   # socket -> bytes
leader_id = None

ok_received = threading.Event()
last_seen = {}

HEARTBEAT_INTERVAL = 1.0
LEADER_TIMEOUT = 30.0

_listener = None

_on_became_leader = None
_on_new_leader = None
_on_game_event = None

_state_lock = threading.Lock()


def recv_line(conn, buf: bytes):
    while b"\n" not in buf:
        data = conn.recv(4096)
        if not data:
            raise ConnectionError("socket closed")
        buf += data
    line, _, rest = buf.partition(b"\n")
    return line.decode(), rest


def send_ndjson(conn, obj):
    conn.sendall((json.dumps(obj) + "\n").encode())


def init_p2p(local_id: int, on_became_leader, on_new_leader, on_game_event):
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


def get_player_ids():
    with _state_lock:
        return sorted(set(list(peers.keys()) + [my_id]))


def get_local_id():
    return my_id


def get_leader_id():
    with _state_lock:
        return leader_id


def broadcast(obj):
    with _state_lock:
        items = list(connections.items())

    for pid, conn in items:
        try:
            send_ndjson(conn, obj)
        except Exception:
            pass

    if "event_name" in obj and _on_game_event:
        _on_game_event(obj)


def send_to(pid: int, obj):
    with _state_lock:
        conn = connections.get(pid)
    if not conn:
        return False
    try:
        send_ndjson(conn, obj)
        return True
    except Exception:
        return False


def connect_to_peer(pid: int, ip: str, port: int):
    with _state_lock:
        if pid in connections:
            return

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((ip, port))
        with _state_lock:
            recv_buffers[s] = b""
            connections[pid] = s
        send_ndjson(s, {"id": my_id})
        print(f"[P2P] Connected to peer {pid} at {ip}:{port}")
    except Exception:
        try:
            s.close()
        except Exception:
            pass


def _announce_player_left(pid: int):
    with _state_lock:
        is_leader = (leader_id == my_id)
    if is_leader:
        ev = make_event(EVENT_PLAYER_LEAVE, {"player_id": pid}, sender=my_id)
        broadcast(ev)


def update_peers_from_bootstrap(new_list):
    """
    new_list is: [(pid, (ip, port)), ...]
    Bootstrap provides the REAL reachable IP/port for each peer.
    """
    global leader_id

    new_ids = set(pid for pid, _ in new_list)

    # Remove stale peers
    with _state_lock:
        stale = [pid for pid in list(peers.keys()) if pid not in new_ids]

    for pid in stale:
        print(f"[P2P] Peer {pid} removed by bootstrap update")

        with _state_lock:
            peers.pop(pid, None)

            conn = connections.pop(pid, None)
            if conn is not None:
                recv_buffers.pop(conn, None)
            last_seen.pop(pid, None)

            was_leader = (leader_id == pid)
            if was_leader:
                leader_id = None

        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

        if was_leader:
            threading.Thread(target=run_bully, daemon=True).start()

        _announce_player_left(pid)

    # ✅ IMPORTANT FIX: use ip/port from bootstrap (NOT 127.0.0.1)
    for pid, (ip, port) in new_list:
        with _state_lock:
            peers[pid] = (ip, port)

        if pid != my_id:
            connect_to_peer(pid, ip, port)

    # Election logic: if we don't know a leader yet, wait briefly
    with _state_lock:
        need_election = (leader_id is None)

    if need_election:
        start = time.time()
        while True:
            with _state_lock:
                if leader_id is not None:
                    return
            if time.time() - start >= 1.5:
                break
            time.sleep(0.05)

        print("[BULLY] No leader announcement received -> starting election.")
        run_bully()


def start_background_threads():
    threading.Thread(target=_listen_to_peers, daemon=True).start()
    threading.Thread(target=_heartbeat_monitor, daemon=True).start()
    threading.Thread(target=_leader_heartbeat, daemon=True).start()


def _accept_peers():
    while True:
        conn, addr = _listener.accept()
        with _state_lock:
            recv_buffers[conn] = b""
        threading.Thread(target=_handle_peer_connection, args=(conn,), daemon=True).start()


def _handle_peer_connection(conn: socket.socket):
    try:
        with _state_lock:
            buf0 = recv_buffers.get(conn, b"")
        line, rest = recv_line(conn, buf0)
    except ConnectionError:
        try:
            conn.close()
        except Exception:
            pass
        return

    with _state_lock:
        recv_buffers[conn] = rest

    msg = json.loads(line)
    pid = msg["id"]

    with _state_lock:
        connections[pid] = conn
        current_leader = leader_id

    print(f"[P2P] Incoming connection from peer {pid}")

    if current_leader is not None:
        try:
            send_ndjson(conn, {"type": "LEADER", "leader": current_leader})
        except Exception:
            pass


def _handle_bully_message(pid, msg):
    global leader_id

    t = msg.get("type")
    if t == "ELECTION":
        with _state_lock:
            conn = connections.get(pid)
        if conn:
            try:
                send_ndjson(conn, {"type": "OK"})
            except Exception:
                pass
        threading.Thread(target=run_bully, daemon=True).start()

    elif t == "OK":
        ok_received.set()

    elif t == "LEADER":
        new_leader = msg.get("leader")
        if new_leader is None:
            return
        with _state_lock:
            leader_id = new_leader
            last_seen[leader_id] = time.time()
        print(f"[BULLY] Leader announced: {leader_id}")
        if _on_new_leader:
            _on_new_leader(leader_id, pid)

    elif t == "HEARTBEAT":
        with _state_lock:
            last_seen[pid] = time.time()


def run_bully():
    global leader_id
    print("[BULLY] Starting election.")

    with _state_lock:
        higher = [pid for pid in peers if pid > my_id]
        ok_received.clear()

    if not higher:
        with _state_lock:
            leader_id = my_id
            last_seen[leader_id] = time.time()
        broadcast({"type": "LEADER", "leader": my_id})
        print("[BULLY] I am the leader! (no higher peers)")
        if _on_became_leader:
            _on_became_leader()
        return

    with _state_lock:
        conns_snapshot = {pid: connections.get(pid) for pid in higher}

    for pid, conn in conns_snapshot.items():
        if conn:
            try:
                send_ndjson(conn, {"type": "ELECTION"})
            except Exception:
                pass

    ok_happened = ok_received.wait(timeout=2)

    if not ok_happened:
        with _state_lock:
            leader_id = my_id
            last_seen[leader_id] = time.time()
        broadcast({"type": "LEADER", "leader": my_id})
        print("[BULLY] I am the leader! (no OK received)")
        if _on_became_leader:
            _on_became_leader()
        return

    print("[BULLY] OK received, waiting for LEADER broadcast")
    start = time.time()
    while True:
        with _state_lock:
            if leader_id is not None:
                return
        if time.time() - start > 3:
            print("[BULLY] LEADER broadcast timeout -> restarting election")
            run_bully()
            return
        time.sleep(0.1)


def _close_and_remove_peer(pid: int, conn: socket.socket, *, may_trigger_election: bool):
    global leader_id

    try:
        conn.close()
    except Exception:
        pass

    with _state_lock:
        if connections.get(pid) is conn:
            connections.pop(pid, None)
        recv_buffers.pop(conn, None)
        last_seen.pop(pid, None)
        peers.pop(pid, None)

        was_leader = (leader_id == pid)
        if was_leader:
            leader_id = None

    if was_leader and may_trigger_election:
        print("[BULLY] Leader disconnected -> starting new election")
        threading.Thread(target=run_bully, daemon=True).start()

    _announce_player_left(pid)


def _listen_to_peers():
    while True:
        with _state_lock:
            items = list(connections.items())

        for pid, conn in items:
            try:
                if conn.fileno() < 0:
                    raise ConnectionError
            except Exception:
                _close_and_remove_peer(pid, conn, may_trigger_election=True)
                continue

            try:
                with _state_lock:
                    buf = recv_buffers.get(conn, b"")

                try:
                    conn.settimeout(0.01)
                except OSError:
                    raise ConnectionError

                try:
                    data = conn.recv(4096)
                except socket.timeout:
                    data = b""
                except (ConnectionResetError, BrokenPipeError, OSError):
                    raise ConnectionError
                finally:
                    try:
                        conn.settimeout(None)
                    except OSError:
                        pass

                if data:
                    buf += data
                    while b"\n" in buf:
                        line, _, buf = buf.partition(b"\n")
                        msg = json.loads(line.decode())

                        with _state_lock:
                            last_seen[pid] = time.time()

                        _handle_message(pid, msg)

                    with _state_lock:
                        recv_buffers[conn] = buf

                if data == b"":
                    if conn.fileno() < 0:
                        raise ConnectionError

            except ConnectionError:
                print(f"[P2P] Lost connection to peer {pid}")
                _close_and_remove_peer(pid, conn, may_trigger_election=True)

        time.sleep(0.1)


def _handle_message(pid, msg):
    if "type" in msg:
        _handle_bully_message(pid, msg)
    elif "event_name" in msg:
        if _on_game_event:
            _on_game_event(msg)


def _heartbeat_monitor():
    global leader_id
    while True:
        with _state_lock:
            lid = leader_id
            lid_last = last_seen.get(lid) if lid is not None else None

        if lid is not None:
            if lid_last is None or (time.time() - lid_last) > LEADER_TIMEOUT:
                print("[HEARTBEAT] Leader timeout -> starting election")
                with _state_lock:
                    leader_id = None
                threading.Thread(target=run_bully, daemon=True).start()

        time.sleep(HEARTBEAT_INTERVAL)


def _leader_heartbeat():
    while True:
        with _state_lock:
            is_leader = (leader_id == my_id)
            conns = list(connections.values())

        if is_leader:
            msg = {"type": "HEARTBEAT"}
            for c in conns:
                try:
                    send_ndjson(c, msg)
                except Exception:
                    pass

        time.sleep(HEARTBEAT_INTERVAL)
