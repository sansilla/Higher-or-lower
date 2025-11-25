import socket
import threading
import json
import time

# NEW: game logic imports + game state
from events import (
    make_event,
    EVENT_GAME_START,
    EVENT_DECK_COMMIT,
    EVENT_DECK_REVEAL,
    EVENT_TURN_START,
    EVENT_GUESS,
    EVENT_RESULT,
    EVENT_TURN_END,
    EVENT_PLAYER_JOIN,
    EVENT_PLAYER_LEAVE,
    EVENT_NEW_LEADER,
)

# === GAME STATE ===
# NEW: shared game state for Higher/Lower
event_log = []  # NEW: simple event log

game_state = {  # NEW
    "players": [],
    "deck": None,
    "current_card": None,
    "current_turn": None,
    "revealed_cards": [],
}

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
        start_game()  # NEW: leader starts the game after election
        return

    # Notify higher-ID peers (original code restored)
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
        print("[BULLY] I am the leader (no OKs)!")
        start_game()  # NEW: also start game in timeout case


def broadcast(obj):
    """
    Sends a message to all connected peers.
    """
    for pid, conn in connections.items():
        try:
            send_ndjson(conn, obj)
        except:
            pass


def start_game():
    """
    Called by the leader to start the game.
    For now: just broadcast the list of players.
    """
    # NEW: avoid duplicates and ensure we include ourselves
    players = sorted(set(list(peers.keys()) + [my_id]))


    event = make_event(
        EVENT_GAME_START,
        {"players": players},
        sender=my_id,
    )

    print(f"[GAME] I am leader, starting game with players: {players}")
    broadcast(event)  # NEW: broadcast GAME_START event to all peers


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
    - Control messages (ELECTION, LEADER, OK)
    - Game events (objects with 'event_name')
    """
    global leader_id

    # Control messages for Bully algorithm
    if "type" in msg:
        if msg["type"] == "ELECTION":
            if my_id > pid:
                # Send OK to lower-ID peer
                send_ndjson(connections[pid], {"type": "OK", "from": my_id})

        elif msg["type"] == "LEADER":
            leader_id = msg["leader"]
            print(f"[BULLY] New leader elected: {leader_id}")

        elif msg["type"] == "OK":
            # Could track that a higher-id peer exists (not needed for now)
            pass

    # Game events
    elif "event_name" in msg:  # NEW: pass game events into event handler
        handle_event(msg)


# NEW: event dispatcher + basic handlers

def handle_event(event):  # NEW
    """
    Handle a game event coming from any peer.
    """
    event_name = event["event_name"]
    payload = event["payload"]
    sender = event["from"]

    event_log.append(event)

    if event_name == EVENT_GAME_START:
        handle_game_start(payload, sender)
    elif event_name == EVENT_DECK_COMMIT:
        handle_deck_commit(payload, sender)
    elif event_name == EVENT_DECK_REVEAL:
        handle_deck_reveal(payload, sender)
    elif event_name == EVENT_TURN_START:
        handle_turn_start(payload, sender)
    elif event_name == EVENT_GUESS:
        handle_guess(payload, sender)
    elif event_name == EVENT_RESULT:
        handle_result(payload, sender)
    elif event_name == EVENT_TURN_END:
        handle_turn_end(payload, sender)
    elif event_name == EVENT_PLAYER_JOIN:
        handle_player_join(payload, sender)
    elif event_name == EVENT_PLAYER_LEAVE:
        handle_player_leave(payload, sender)
    elif event_name == EVENT_NEW_LEADER:
        handle_new_leader(payload, sender)


def handle_game_start(payload, sender):  # NEW
    players = payload.get("players", [])
    game_state["players"] = players
    print(f"[GAME] Game started by {sender}. Players: {players}")


def handle_deck_commit(payload, sender):  # NEW
    print(f"[GAME] Deck committed by {sender}")
    game_state["deck"] = payload.get("deck")


def handle_deck_reveal(payload, sender):  # NEW
    card = payload.get("card")
    print(f"[GAME] Card revealed: {card}")
    game_state["current_card"] = card
    game_state["revealed_cards"].append(card)


def handle_turn_start(payload, sender):  # NEW
    turn_player = payload.get("player")
    print(f"[GAME] Turn start for player {turn_player}")
    game_state["current_turn"] = turn_player


def handle_guess(payload, sender):  # NEW
    print(f"[GAME] Guess from {sender}: {payload}")


def handle_result(payload, sender):  # NEW
    print(f"[GAME] Result: {payload}")


def handle_turn_end(payload, sender):  # NEW
    print(f"[GAME] Turn ended: {payload}")


def handle_player_join(payload, sender):  # NEW
    pid = payload.get("player_id")
    if pid is not None and pid not in game_state["players"]:
        game_state["players"].append(pid)
    print(f"[GAME] Player joined: {pid}")


def handle_player_leave(payload, sender):  # NEW
    pid = payload.get("player_id")
    if pid in game_state["players"]:
        game_state["players"].remove(pid)
    print(f"[GAME] Player left: {pid}")


def handle_new_leader(payload, sender):  # NEW
    new_leader = payload.get("leader")
    print(f"[GAME] New leader announced by {sender}: {new_leader}")


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
