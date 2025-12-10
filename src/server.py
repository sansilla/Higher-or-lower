import socket
import threading
import json

LISTEN_IP = "0.0.0.0"
LISTEN_PORT = 1234

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind((LISTEN_IP, LISTEN_PORT))
server.listen()

print(f"[BOOTSTRAP] Listening on {LISTEN_IP}:{LISTEN_PORT}")

clients = []
client_ids = {}
next_id = 1

def send_ndjson(conn, obj):
    conn.sendall((json.dumps(obj) + "\n").encode())

def handle_client(conn, addr):
    global next_id

    print(f"[BOOTSTRAP] New connection from {addr}")

    cid = next_id
    next_id += 1
    client_ids[conn] = cid

    conn.recv(16)

    send_ndjson(conn, {"your_id": cid})

    clients.append((cid, addr))

    peer_packet = {"peers": clients}
    for c in list(client_ids.keys()):
        try:
            send_ndjson(c, peer_packet)
        except Exception:
            pass

while True:
    conn, addr = server.accept()
    threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
