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

def handle_client(conn, addr):
    global next_id

    print(f"[BOOTSTRAP] New connection from {addr}")

    # assign ID
    cid = next_id
    next_id += 1
    client_ids[conn] = cid

    # wait for "READY"
    conn.recv(16)

    # send assigned ID
    conn.send(json.dumps({"your_id": cid}).encode())

    # register client
    clients.append((cid, addr))

    # send peer list to everyone
    peer_packet = json.dumps({"peers": clients}).encode()
    for c, _ in clients:
        try:
            [k.send(peer_packet) for k in client_ids if client_ids[k] == c]
        except:
            pass

while True:
    conn, addr = server.accept()
    threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
