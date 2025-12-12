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

clients = []          # list of (cid, addr)
client_ids = {}       # conn -> cid
next_id = 1
lock = threading.Lock()

def send_ndjson(conn, obj):
    conn.sendall((json.dumps(obj) + "\n").encode())

def broadcast_peers():
    with lock:
        peer_packet = {"peers": list(clients)}
        conns = list(client_ids.keys())
    for c in conns:
        try:
            send_ndjson(c, peer_packet)
        except Exception:
            pass

def handle_client(conn, addr):
    global next_id
    print(f"[BOOTSTRAP] New connection from {addr}")

    with lock:
        cid = next_id
        next_id += 1
        client_ids[conn] = cid

    try:
        conn.recv(16)  # expect READY
        send_ndjson(conn, {"your_id": cid})

        with lock:
            clients.append((cid, addr))

        broadcast_peers()

        # Keep connection open; detect disconnect
        while True:
            data = conn.recv(1)
            if not data:
                raise ConnectionError("client disconnected")

    except Exception:
        print(f"[BOOTSTRAP] Client {cid} disconnected")

    finally:
        with lock:
            # remove from structures
            client_ids.pop(conn, None)
            clients[:] = [(i, a) for (i, a) in clients if i != cid]
        try:
            conn.close()
        except Exception:
            pass
        broadcast_peers()

while True:
    conn, addr = server.accept()
    threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
