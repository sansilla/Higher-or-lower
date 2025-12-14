import socket
import threading
import json

LISTEN_IP = "0.0.0.0"
LISTEN_PORT = 1234

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((LISTEN_IP, LISTEN_PORT))
server.listen()

print(f"[BOOTSTRAP] Listening on {LISTEN_IP}:{LISTEN_PORT}")

clients = []            # [(cid, (ip, p2p_port))]
client_ids = {}         # conn -> cid
next_id = 1
lock = threading.Lock()

token_to_id = {}        # token -> cid
id_to_username = {}     # cid -> username


def send_ndjson(conn, obj):
    conn.sendall((json.dumps(obj) + "\n").encode())


def broadcast_membership():
    with lock:
        packet = {
            "peers": list(clients),              # [(id, (ip, port)), ...]
            "usernames": dict(id_to_username),
        }
        print("[BOOTSTRAP] peers broadcast:", packet["peers"])
        conns = list(client_ids.keys())

    for c in conns:
        try:
            send_ndjson(c, packet)
        except Exception:
            pass


def recv_line(conn, buf: bytes):
    while b"\n" not in buf:
        data = conn.recv(4096)
        if not data:
            raise ConnectionError
        buf += data
    line, _, rest = buf.partition(b"\n")
    return line.decode(), rest


def handle_client(conn, addr):
    global next_id, clients

    print(f"[BOOTSTRAP] New connection from {addr}")
    buf = b""
    cid = None

    try:
        line, buf = recv_line(conn, buf)
        line = line.strip()

        token = None
        username = None

        # Accept BOTH:
        #   old client: "READY"
        #   new client: {"type":"READY","token":"...","username":"..."}
        if line == "READY":
            pass
        else:
            msg = json.loads(line)
            if msg.get("type") == "READY":
                token = msg.get("token")
                username = msg.get("username")

        with lock:
            # Reuse ID on reconnect (token)
            if token and token in token_to_id:
                cid = token_to_id[token]
            else:
                cid = next_id
                next_id += 1
                if token:
                    token_to_id[token] = cid

            if not username:
                username = f"p{cid}"
            id_to_username[cid] = username

            client_ids[conn] = cid

            # ✅ IMPORTANT FIX:
            # P2P listener port is deterministic: 50000 + cid
            # addr[1] is EPHEMERAL bootstrap client port -> do NOT use it
            p2p_port = 50000 + cid
            peer_addr = (addr[0], p2p_port)

            # reconnect-safe: replace any existing entry for cid
            clients[:] = [(i, a) for (i, a) in clients if i != cid]
            clients.append((cid, peer_addr))

        send_ndjson(conn, {"your_id": cid})
        broadcast_membership()

        # keep alive
        while True:
            data = conn.recv(1)
            if not data:
                raise ConnectionError

    except Exception:
        print(f"[BOOTSTRAP] Client {cid} disconnected")

    finally:
        with lock:
            client_ids.pop(conn, None)
            clients[:] = [(i, a) for (i, a) in clients if i != cid]
            if cid is not None:
                id_to_username.pop(cid, None)

        try:
            conn.close()
        except Exception:
            pass

        broadcast_membership()


while True:
    conn, addr = server.accept()
    threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
