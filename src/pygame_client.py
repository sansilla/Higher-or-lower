import socket
import json
import random
import threading

import pygame

from events import (
    make_event,
    EVENT_GAME_START,
    EVENT_DECK_COMMIT,
    EVENT_DECK_REVEAL,
    EVENT_TURN_START,
    EVENT_NEW_LEADER,
    EVENT_GUESS,
    EVENT_RESULT,
)
from game_logic import game_state, event_log, handle_event
from game_logic.cards import card_str, build_deck
from p2p import (
    init_p2p,
    start_background_threads,
    update_peers_from_bootstrap,
    recv_line,
    broadcast,
    get_player_ids,
    get_local_id,
    get_leader_id,
)

BOOTSTRAP_HOST = "127.0.0.1"
BOOTSTRAP_PORT = 1234


# === P2P CALLBACKS ===

def on_became_leader():
    """
    Called when THIS node wins the Bully election.
    Start the game only if there are at least 2 players.
    """
    players = get_player_ids()
    local_id = get_local_id()

    if len(players) < 2:
        print(f"[GAME] I am leader (id={local_id}) but only {len(players)} player(s). Waiting for more players.")
        return

    game_state["players"] = players

    # 1) GAME_START
    game_start = make_event(
        EVENT_GAME_START,
        {"players": players},
        sender=local_id,
    )
    print(f"[GAME] I am leader, starting game with players: {players}")
    broadcast(game_start)

    # 2) Deck commit (deterministic deck via seed)
    seed = random.randint(0, 2**31 - 1)
    deck = build_deck(seed)
    game_state["deck"] = deck
    game_state["revealed_cards"] = []

    deck_event = make_event(
        EVENT_DECK_COMMIT,
        {"seed": seed},
        sender=local_id,
    )
    broadcast(deck_event)

    # 3) Reveal first card
    first_card = deck[0]
    game_state["current_card"] = first_card
    game_state["revealed_cards"].append(first_card)

    reveal_event = make_event(
        EVENT_DECK_REVEAL,
        {"card": first_card},
        sender=local_id,
    )
    broadcast(reveal_event)

    # 4) Start first turn
    first_player = players[0]
    game_state["current_turn"] = first_player

    turn_event = make_event(
        EVENT_TURN_START,
        {"player": first_player},
        sender=local_id,
    )
    broadcast(turn_event)


def on_new_leader(leader_id, sender):
    """
    Translate leader election into a game-level NEW_LEADER event,
    so the game logic can react if needed.
    """
    ev = make_event(
        EVENT_NEW_LEADER,
        {"leader": leader_id},
        sender=sender,
    )
    handle_event(ev)


# === BOOTSTRAP CONNECTION ===

bs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
bs.connect((BOOTSTRAP_HOST, BOOTSTRAP_PORT))
bs.send(b"READY")  # Notify bootstrap server

bs_buf = b""
line, bs_buf = recv_line(bs, bs_buf)
obj = json.loads(line)
my_id = obj["your_id"]
print(f"[CLIENT] My ID = {my_id}")


# === INIT P2P LAYER ===

init_p2p(
    local_id=my_id,
    on_became_leader=on_became_leader,
    on_new_leader=on_new_leader,
    on_game_event=handle_event,
)
start_background_threads()

print("[CLIENT] Waiting for peer list from bootstrap...")


def bootstrap_loop():
    """
    Background thread:
    - reads peer list updates from bootstrap server
    - passes them to p2p.update_peers_from_bootstrap
    """
    global bs_buf
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
                update_peers_from_bootstrap(new_list)

        except ConnectionError:
            print("[CLIENT] Bootstrap server disconnected. Exiting bootstrap loop.")
            break


threading.Thread(target=bootstrap_loop, daemon=True).start()


# === PYGAME SETUP ===

pygame.init()
WIDTH, HEIGHT = 700, 450
screen = pygame.display.set_mode((WIDTH, HEIGHT))
pygame.display.set_caption(f"Higher or Lower - Player {my_id}")
font = pygame.font.SysFont(None, 28)
big_font = pygame.font.SysFont(None, 40)
clock = pygame.time.Clock()

# Buttons
button_higher = pygame.Rect(100, 320, 180, 60)
button_lower = pygame.Rect(400, 320, 180, 60)


def draw_text(text, x, y, fnt=None, color=(255, 255, 255)):
    if fnt is None:
        fnt = font
    img = fnt.render(text, True, color)
    screen.blit(img, (x, y))


def get_last_result():
    """
    Look at event_log and return info about the last RESULT event, if any.
    """
    for ev in reversed(event_log):
        if ev["event_name"] == EVENT_RESULT:
            p = ev["payload"]
            return p
    return None


running = True
while running:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False

        elif event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
            mx, my = event.pos

            # Only allow guessing if it's our turn
            if game_state.get("current_turn") == get_local_id():
                if button_higher.collidepoint(mx, my):
                    guess_ev = make_event(
                        EVENT_GUESS,
                        {"guess": "HIGHER"},
                        sender=get_local_id(),
                    )
                    broadcast(guess_ev)
                elif button_lower.collidepoint(mx, my):
                    guess_ev = make_event(
                        EVENT_GUESS,
                        {"guess": "LOWER"},
                        sender=get_local_id(),
                    )
                    broadcast(guess_ev)

    # === DRAW UI ===
    screen.fill((20, 20, 20))

    # Leader info
    leader = get_leader_id()
    draw_text(f"Your ID: {get_local_id()}", 20, 10)
    draw_text(f"Leader: {leader if leader is not None else 'None'}", 20, 40)

    # Current card
    card = game_state.get("current_card")
    if card is not None:
        draw_text("Current card:", 20, 90)
        draw_text(card_str(card), 180, 85, big_font)
    else:
        draw_text("Waiting for game to start...", 20, 90)

    # Turn info
    current_turn = game_state.get("current_turn")
    if current_turn is not None:
        if current_turn == get_local_id():
            turn_str = "YOUR TURN"
            color = (50, 220, 50)
        else:
            turn_str = f"Turn: Player {current_turn}"
            color = (200, 200, 50)
        draw_text(turn_str, 20, 130, big_font, color)
    else:
        draw_text("No active turn.", 20, 130)

    # Last result
    last_res = get_last_result()
    if last_res:
        player = last_res.get("player")
        correct = last_res.get("correct")
        prev = last_res.get("prev")
        new = last_res.get("new")
        guess = last_res.get("guess")

        prev_str = card_str(prev)
        new_str = card_str(new)
        res_text = f"P{player} guessed {guess}: {'CORRECT' if correct else 'WRONG'} ({prev_str} → {new_str})"
        color = (50, 220, 50) if correct else (220, 50, 50)
        draw_text("Last result:", 20, 180)
        draw_text(res_text, 20, 210, color=color)

    # Buttons
    is_my_turn = (game_state.get("current_turn") == get_local_id())
    btn_color_h = (70, 70, 200) if is_my_turn else (50, 50, 100)
    btn_color_l = (70, 200, 70) if is_my_turn else (50, 100, 50)

    pygame.draw.rect(screen, btn_color_h, button_higher)
    pygame.draw.rect(screen, btn_color_l, button_lower)
    draw_text("HIGHER", button_higher.x + 40, button_higher.y + 18)
    draw_text("LOWER", button_lower.x + 45, button_lower.y + 18)

    if not is_my_turn:
        draw_text("You can only guess on YOUR turn.", 20, 260)

    pygame.display.flip()
    clock.tick(30)

pygame.quit()
