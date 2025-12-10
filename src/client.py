import socket
import json
import time
import random

from events import (
    make_event,
    EVENT_GAME_START,
    EVENT_DECK_COMMIT,
    EVENT_DECK_REVEAL,
    EVENT_TURN_START,
    EVENT_NEW_LEADER,
)

from game_logic import game_state, event_log, handle_event
from game_logic.cards import build_deck
from p2p import (
    init_p2p,
    start_background_threads,
    update_peers_from_bootstrap,
    recv_line,
    broadcast,
    get_player_ids,
)

BOOTSTRAP_HOST = "127.0.0.1"
BOOTSTRAP_PORT = 1234


# === CALLBACKS FOR P2P LAYER ===

def on_became_leader():
    """
    Called by the P2P layer when THIS node wins the Bully election.
    Now actually starts the game: GAME_START + DECK_COMMIT + first DECK_REVEAL + TURN_START.
    """
    players = get_player_ids()
    game_state["players"] = players

    # 1) GAME_START
    game_start = make_event(
        EVENT_GAME_START,
        {"players": players},
        sender=my_id,
    )
    print(f"[GAME] I am leader, starting game with players: {players}")
    broadcast(game_start)

    # 2) Deck commit with shared seed (all nodes will reconstruct)
    seed = random.randint(0, 2**31 - 1)
    deck = build_deck(seed)
    game_state["deck"] = deck
    game_state["revealed_cards"] = []

    deck_event = make_event(
        EVENT_DECK_COMMIT,
        {"seed": seed},
        sender=my_id,
    )
    broadcast(deck_event)

    # 3) Reveal first card so there is a baseline for higher/lower
    first_card = deck[0]
    game_state["current_card"] = first_card
    game_state["revealed_cards"].append(first_card)

    reveal_event = make_event(
        EVENT_DECK_REVEAL,
        {"card": first_card},
        sender=my_id,
    )
    broadcast(reveal_event)

    # 4) Start first turn: first player in list
    first_player = players[0]
    game_state["current_turn"] = first_player

    turn_event = make_event(
        EVENT_TURN_START,
        {"player": first_player},
        sender=my_id,
    )
    broadcast(turn_event)


def on_new_leader(leader_id, sender):
    """
    Called when some node announces itself as leader.
    We turn this into a game-level NEW_LEADER event so game_logic can see it.
    """
    event = make_event(
        EVENT_NEW_LEADER,
        {"leader": leader_id},
        sender=sender,
    )
    handle_event(event)


# === CONNECT TO BOOTSTRAP SERVER ===

bs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
bs.connect((BOOTSTRAP_HOST, BOOTSTRAP_PORT))
bs.send(b"READY")  # Notify bootstrap server

# Receive assigned ID from bootstrap (using recv_line from p2p)
bs_buf = b""
line, bs_buf = recv_line(bs, bs_buf)
obj = json.loads(line)
my_id = obj["your_id"]
print(f"[CLIENT] My ID = {my_id}")

# === INITIALIZE P2P LAYER ===

init_p2p(
    local_id=my_id,
    on_became_leader=on_became_leader,
    on_new_leader=on_new_leader,
    on_game_event=handle_event,
)

start_background_threads()

print("[CLIENT] Waiting for peer list from bootstrap...")

# === MAIN LOOP: HANDLE UPDATED PEER LIST FROM BOOTSTRAP ===

while True:
    try:
        # accumulate until we have at least one full line
        while b"\n" not in bs_buf:
            data = bs.recv(4096)
            if not data:
                raise ConnectionError("bootstrap closed")
            bs_buf += data

        # process all complete lines
        while b"\n" in bs_buf:
            line, sep, bs_buf = bs_buf.partition(b"\n")
            parsed = json.loads(line.decode())
            new_list = parsed["peers"]

            update_peers_from_bootstrap(new_list)

    except ConnectionError:
        print("[CLIENT] Bootstrap server disconnected. Exiting.")
        break
