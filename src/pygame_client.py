import socket
import json
import random
import threading
import os

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
small_font = pygame.font.SysFont(None, 24)
clock = pygame.time.Clock()

# === CARD IMAGE LOADING ===

CARD_IMAGES = {}  # (rank, suit) -> pygame.Surface

BASE_DIR = os.path.dirname(__file__)
CARD_IMAGE_DIR = os.path.join(BASE_DIR, "assets", "cards")


def _card_filename(card):
    """
    card: (rank, suit) e.g. (1, 'H') or [1, 'H']
    Returns filename like 'AH.png', '10H.png', etc.
    """
    if isinstance(card, list):
        card = tuple(card)
    rank, suit = card

    rank_map = {1: "A", 11: "J", 12: "Q", 13: "K"}
    r = rank_map.get(rank, str(rank))  # 2–10 stay numeric
    return f"{r}{suit}.png"


def get_card_image(card):
    """
    Returns a pygame.Surface for the given card, loading and caching if needed.
    If image is missing, returns None.
    """
    if isinstance(card, list):
        card = tuple(card)

    key = card
    if key in CARD_IMAGES:
        return CARD_IMAGES[key]

    filename = _card_filename(card)
    path = os.path.join(CARD_IMAGE_DIR, filename)

    if not os.path.exists(path):
        print(f"[PYGAME] Card image not found: {path}")
        return None

    try:
        img = pygame.image.load(path).convert_alpha()
        img = pygame.transform.smoothscale(img, (200, 280))
        CARD_IMAGES[key] = img
        return img
    except Exception as e:
        print(f"[PYGAME] Failed to load card image {path}: {e}")
        return None


# Buttons
button_higher = pygame.Rect(100, 360, 180, 60)
button_lower = pygame.Rect(400, 360, 180, 60)


def draw_text(text, x, y, fnt=None, color=(255, 255, 255)):
    if fnt is None:
        fnt = font
    img = fnt.render(text, True, color)
    screen.blit(img, (x, y))


def draw_text_center(text, center_x, y, fnt=None, color=(255, 255, 255)):
    if fnt is None:
        fnt = font
    img = fnt.render(text, True, color)
    rect = img.get_rect()
    rect.midtop = (center_x, y)
    screen.blit(img, rect)


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

    # Top-left info
    leader = get_leader_id()
    draw_text(f"Your ID: {get_local_id()}", 20, 10)
    draw_text(f"Leader: {leader if leader is not None else 'None'}", 20, 40)

    # Card + turn text
    card = game_state.get("current_card")
    card_rect = None
    if card is not None:
        img = get_card_image(card)
        if img is not None:
            card_rect = img.get_rect()
            card_rect.centerx = WIDTH // 2
            card_rect.top = 70
            screen.blit(img, card_rect)
        else:
            # Fallback text card (centered)
            text = card_str(card)
            draw_text_center(text, WIDTH // 2, 140, big_font)

    # Turn info (centered above card)
    current_turn = game_state.get("current_turn")
    if current_turn is not None:
        if current_turn == get_local_id():
            turn_str = "YOUR TURN"
            color = (50, 220, 50)
        else:
            turn_str = f"Turn: Player {current_turn}"
            color = (200, 200, 50)
    else:
        turn_str = "Waiting for game to start..."
        color = (200, 200, 200)

    draw_text_center(turn_str, WIDTH // 2, 40, big_font, color)

    # Last result: on the right side of the card
    last_res = get_last_result()
    if last_res and card_rect is not None:
        player = last_res.get("player")
        correct = last_res.get("correct")
        prev = last_res.get("prev")
        new = last_res.get("new")
        guess = last_res.get("guess")

        prev_str = card_str(prev)
        new_str = card_str(new)

        base_x = card_rect.right + 20
        base_y = card_rect.top + 20

        result_color = (50, 220, 50) if correct else (220, 50, 50)

        draw_text("Last result:", base_x, base_y, font)
        draw_text(f"Player {player} guessed {guess}", base_x, base_y + 25, small_font, (200, 200, 200))
        draw_text(f"{prev_str} -> {new_str}", base_x, base_y + 50, small_font, (220, 220, 220))
        draw_text("CORRECT" if correct else "WRONG", base_x, base_y + 75, small_font, result_color)

    # Buttons
    is_my_turn = (game_state.get("current_turn") == get_local_id())
    btn_color_h = (70, 70, 200) if is_my_turn else (50, 50, 100)
    btn_color_l = (70, 200, 70) if is_my_turn else (50, 100, 50)

    pygame.draw.rect(screen, btn_color_h, button_higher)
    pygame.draw.rect(screen, btn_color_l, button_lower)
    draw_text("HIGHER", button_higher.x + 40, button_higher.y + 18)
    draw_text("LOWER", button_lower.x + 45, button_lower.y + 18)

    pygame.display.flip()
    clock.tick(30)

pygame.quit()
