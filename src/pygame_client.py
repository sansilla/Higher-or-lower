import socket
import json
import random
import threading
import os
import sys
import uuid

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
    EVENT_STATE_REQUEST,
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
    get_local_id,
    get_leader_id,
)

BOOTSTRAP_HOST = "127.0.0.1"
BOOTSTRAP_PORT = 1234

BASE_DIR = os.path.dirname(__file__)

# username passed on command line
username = sys.argv[1] if len(sys.argv) > 1 else "default"

# bootstrap-supplied: {id:int -> username:str}
usernames_map = {}

# ================= PERSISTENT TOKEN =================

TOKENS_DIR = os.path.join(BASE_DIR, "tokens")
os.makedirs(TOKENS_DIR, exist_ok=True)

token_path = os.path.join(TOKENS_DIR, f"{username}.token")

if os.path.exists(token_path):
    with open(token_path, "r", encoding="utf-8") as f:
        token = f.read().strip()
else:
    token = str(uuid.uuid4())
    with open(token_path, "w", encoding="utf-8") as f:
        f.write(token)


def display_name(pid: int | None) -> str:
    if pid is None:
        return "?"
    if pid == get_local_id():
        return username
    u = usernames_map.get(pid)
    return u if u else f"p{pid}"


# ================= P2P CALLBACKS =================

def on_became_leader():
    players = get_player_ids()
    local_id = get_local_id()

    # don't restart if already started
    if game_state.get("deck_seed") is not None and len(game_state.get("revealed_cards", [])) > 0:
        return

    if len(players) < 2:
        return

    broadcast(make_event(EVENT_GAME_START, {"players": players}, sender=local_id))
    seed = random.randint(0, 2**31 - 1)
    broadcast(make_event(EVENT_DECK_COMMIT, {"seed": seed}, sender=local_id))
    deck = build_deck(seed)
    broadcast(make_event(EVENT_DECK_REVEAL, {"card": deck[0]}, sender=local_id))
    broadcast(make_event(EVENT_TURN_START, {"player": players[0]}, sender=local_id))


def on_new_leader(leader_id, sender):
    handle_event(make_event(EVENT_NEW_LEADER, {"leader": leader_id}, sender=sender))


# ================= BOOTSTRAP =================

bs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
bs.connect((BOOTSTRAP_HOST, BOOTSTRAP_PORT))

# Send READY with token + username (server also supports plain "READY")
bs.sendall((json.dumps({"type": "READY", "token": token, "username": username}) + "\n").encode())

bs_buf = b""
line, bs_buf = recv_line(bs, bs_buf)
my_id = json.loads(line)["your_id"]

init_p2p(
    local_id=my_id,
    on_became_leader=on_became_leader,
    on_new_leader=on_new_leader,
    on_game_event=handle_event,
)
start_background_threads()


def bootstrap_loop():
    global bs_buf, usernames_map
    while True:
        try:
            while b"\n" not in bs_buf:
                data = bs.recv(4096)
                if not data:
                    raise ConnectionError
                bs_buf += data

            while b"\n" in bs_buf:
                raw, _, bs_buf = bs_buf.partition(b"\n")
                if not raw.strip():
                    continue
                msg = json.loads(raw.decode())

                # peers stays old shape: [(id, addr), ...]
                if "peers" in msg:
                    update_peers_from_bootstrap(msg["peers"])

                # usernames is optional; normalize keys to int
                um = msg.get("usernames")
                if isinstance(um, dict):
                    usernames_map = {int(k): v for k, v in um.items()}

        except ConnectionError:
            break
        except Exception:
            # never kill the whole UI because of a bad membership line
            continue


threading.Thread(target=bootstrap_loop, daemon=True).start()


# ================= PYGAME =================

pygame.init()
WIDTH, HEIGHT = 700, 450
screen = pygame.display.set_mode((WIDTH, HEIGHT))
pygame.display.set_caption(f"Higher or Lower - {username} (ID {my_id})")

font = pygame.font.SysFont(None, 28)
big_font = pygame.font.SysFont(None, 40)
small_font = pygame.font.SysFont(None, 24)

# bigger username label (top-left)
username_font = pygame.font.SysFont(None, 50)

clock = pygame.time.Clock()


# ================= CARD IMAGES =================

CARD_IMAGES = {}
CARD_IMAGE_DIR = os.path.join(BASE_DIR, "assets", "cards")
CARD_SIZE = (200, 280)


def _card_filename(card):
    if isinstance(card, list):
        card = tuple(card)
    r, s = card
    rank = {1: "A", 11: "J", 12: "Q", 13: "K"}.get(r, str(r))
    return f"{rank}{s}.png"


def get_card_image(card):
    if isinstance(card, list):
        card = tuple(card)

    if card in CARD_IMAGES:
        return CARD_IMAGES[card]

    path = os.path.join(CARD_IMAGE_DIR, _card_filename(card))
    if not os.path.exists(path):
        return None

    img = pygame.image.load(path).convert_alpha()
    img = pygame.transform.smoothscale(img, CARD_SIZE)
    CARD_IMAGES[card] = img
    return img


# ================= BUTTON IMAGES =================

BUTTON_SIZE = (180, 60)
BUTTON_DIR = os.path.join(BASE_DIR, "assets", "buttons")

BTN_HIGHER = pygame.transform.smoothscale(
    pygame.image.load(os.path.join(BUTTON_DIR, "higher.png")).convert_alpha(),
    BUTTON_SIZE
)
BTN_LOWER = pygame.transform.smoothscale(
    pygame.image.load(os.path.join(BUTTON_DIR, "lower.png")).convert_alpha(),
    BUTTON_SIZE
)


def make_disabled(surface: pygame.Surface) -> pygame.Surface:
    """
    Darken the PNG itself (preserve rounded alpha), so no black rectangle outline.
    """
    s = surface.copy()
    # darker, close to background tone
    s.fill((95, 95, 95, 255), special_flags=pygame.BLEND_RGBA_MULT)
    return s


BTN_HIGHER_DISABLED = make_disabled(BTN_HIGHER)
BTN_LOWER_DISABLED = make_disabled(BTN_LOWER)


# ================= STACK / ANIMATION =================

PILE_MAX = 6
PILE_DX = 6
PILE_DY = 5
PILE_ANGLE = 2
ANIM_TIME = 0.25

anim = {"active": False}
seen_reveals = 0
pending_reveals = []


def lerp(a, b, t):
    return a + (b - a) * t


def pile_pose(i):
    x = WIDTH // 2 - CARD_SIZE[0] // 2 + i * PILE_DX
    y = 70 + i * PILE_DY
    direction = -1 if i % 2 == 0 else 1
    angle = direction * (1 + i // 2) * PILE_ANGLE
    return x, y, angle


# ================= BUTTON RECTS =================

button_higher = pygame.Rect(100, 360, 180, 60)
button_lower = pygame.Rect(400, 360, 180, 60)


def draw_text(text, x, y, fnt=font, color=(255, 255, 255)):
    screen.blit(fnt.render(text, True, color), (x, y))


def draw_text_center(text, y, fnt=big_font, color=(255, 255, 255)):
    img = fnt.render(text, True, color)
    screen.blit(img, img.get_rect(midtop=(WIDTH // 2, y)))


def get_last_result():
    for ev in reversed(event_log):
        if ev["event_name"] == EVENT_RESULT:
            return ev["payload"]
    return None


# ================= SNAPSHOT REQUEST (RECONNECT SYNC) =================

requested_snapshot = False


def maybe_request_snapshot():
    global requested_snapshot
    leader = get_leader_id()
    local = get_local_id()

    if leader is None:
        return
    if leader == local:
        return
    if requested_snapshot:
        return
    if game_state.get("deck") is not None:
        return

    requested_snapshot = True
    broadcast(make_event(EVENT_STATE_REQUEST, {}, sender=local))


def maybe_start_game_if_leader():
    if get_leader_id() != get_local_id():
        return
    if game_state.get("deck_seed") is not None and len(game_state.get("revealed_cards", [])) > 0:
        return
    players = get_player_ids()
    if len(players) < 2:
        return
    on_became_leader()


# ================= MAIN LOOP =================

running = True
while running:
    mx, my = pygame.mouse.get_pos()

    for e in pygame.event.get():
        if e.type == pygame.QUIT:
            running = False

        elif e.type == pygame.MOUSEBUTTONDOWN and e.button == 1:
            if game_state.get("current_turn") == get_local_id():
                if button_higher.collidepoint(e.pos):
                    broadcast(make_event(EVENT_GUESS, {"guess": "HIGHER"}, sender=get_local_id()))
                elif button_lower.collidepoint(e.pos):
                    broadcast(make_event(EVENT_GUESS, {"guess": "LOWER"}, sender=get_local_id()))

    maybe_request_snapshot()
    maybe_start_game_if_leader()

    screen.fill((20, 20, 20))

    # Top-left info
    draw_text(username, 20, 6, fnt=username_font)
    draw_text(f"Your ID: {get_local_id()}", 20, 56)
    draw_text(f"Leader: {get_leader_id()}", 20, 80)

    # Turn text: <username>'s turn (real names when known)
    turn = game_state.get("current_turn")
    if turn is not None:
        draw_text_center(f"{display_name(turn)}'s turn", 40, color=(200, 200, 50))
    else:
        draw_text_center("Waiting...", 40, color=(200, 200, 200))

    # --- Reveal queue update ---
    revealed_all = game_state.get("revealed_cards", [])

    if len(revealed_all) < seen_reveals:
        seen_reveals = len(revealed_all)
        pending_reveals.clear()
        anim = {"active": False}

    if len(revealed_all) > seen_reveals:
        pending_reveals.extend(revealed_all[seen_reveals:])
        seen_reveals = len(revealed_all)

    now = pygame.time.get_ticks() / 1000.0

    if (not anim["active"]) and pending_reveals:
        next_card = pending_reveals.pop(0)

        finished_count = seen_reveals - len(pending_reveals) - 1
        top_index = min(finished_count, PILE_MAX - 1)

        tx, ty, ta = pile_pose(top_index)
        anim = {
            "active": True,
            "card": next_card,
            "t0": now,
            "from": (tx, -300),
            "to": (tx, ty),
            "from_a": -15,
            "to_a": ta,
        }

    started_not_finished = 1 if anim["active"] else 0
    finished_count = seen_reveals - len(pending_reveals) - started_not_finished
    if finished_count < 0:
        finished_count = 0

    finished_cards = revealed_all[:finished_count]
    draw_pile = finished_cards[-PILE_MAX:]

    for i, c in enumerate(draw_pile):
        img = get_card_image(c)
        if not img:
            continue
        x, y, a = pile_pose(i)
        screen.blit(pygame.transform.rotozoom(img, a, 1), (x, y))

    if anim["active"]:
        t = min((now - anim["t0"]) / ANIM_TIME, 1.0)
        t_smooth = t * t * (3 - 2 * t)

        x = lerp(anim["from"][0], anim["to"][0], t_smooth)
        y = lerp(anim["from"][1], anim["to"][1], t_smooth)
        a = lerp(anim["from_a"], anim["to_a"], t_smooth)

        img = get_card_image(anim["card"])
        if img:
            screen.blit(pygame.transform.rotozoom(img, a, 1), (x, y))

        if t >= 1.0:
            anim["active"] = False

    # Last result (top-right)
    res = get_last_result()
    if res:
        margin = 20
        line_h = 26
        lines = [
            "Last result:",
            f"{display_name(res['player'])} guessed {res['guess']}",
            "CORRECT" if res["correct"] else "WRONG",
        ]
        colors = [
            (255, 255, 255),
            (220, 220, 220),
            (50, 220, 50) if res["correct"] else (220, 50, 50),
        ]
        for i, (text, col) in enumerate(zip(lines, colors)):
            img = (font if i == 0 else small_font).render(text, True, col)
            x = WIDTH - margin - img.get_width()
            y = margin + i * line_h
            screen.blit(img, (x, y))

    # Buttons: use darkened PNG when inactive (no outline)
    is_my_turn = (game_state.get("current_turn") == get_local_id())

    if is_my_turn:
        screen.blit(BTN_HIGHER, button_higher)
        screen.blit(BTN_LOWER, button_lower)
        if button_higher.collidepoint(mx, my):
            screen.blit(BTN_HIGHER, button_higher.move(0, -3))
        if button_lower.collidepoint(mx, my):
            screen.blit(BTN_LOWER, button_lower.move(0, -3))
    else:
        screen.blit(BTN_HIGHER_DISABLED, button_higher)
        screen.blit(BTN_LOWER_DISABLED, button_lower)

    pygame.display.flip()
    clock.tick(60)

pygame.quit()
