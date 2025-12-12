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


# ================= P2P CALLBACKS =================

def on_became_leader():
    """
    IMPORTANT:
    Do NOT manually mutate deck/current_card/revealed_cards here.
    broadcast() delivers events locally too; mutating + broadcasting duplicates state.
    """
    players = get_player_ids()
    local_id = get_local_id()

    if len(players) < 2:
        return

    broadcast(make_event(EVENT_GAME_START, {"players": players}, sender=local_id))

    seed = random.randint(0, 2**31 - 1)
    broadcast(make_event(EVENT_DECK_COMMIT, {"seed": seed}, sender=local_id))

    deck = build_deck(seed)
    first_card = deck[0]
    broadcast(make_event(EVENT_DECK_REVEAL, {"card": first_card}, sender=local_id))

    first_player = players[0]
    broadcast(make_event(EVENT_TURN_START, {"player": first_player}, sender=local_id))


def on_new_leader(leader_id, sender):
    handle_event(make_event(EVENT_NEW_LEADER, {"leader": leader_id}, sender=sender))


# ================= BOOTSTRAP =================

bs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
bs.connect((BOOTSTRAP_HOST, BOOTSTRAP_PORT))
bs.send(b"READY")

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
    global bs_buf
    while True:
        try:
            while b"\n" not in bs_buf:
                data = bs.recv(4096)
                if not data:
                    raise ConnectionError
                bs_buf += data

            while b"\n" in bs_buf:
                line, _, bs_buf = bs_buf.partition(b"\n")
                update_peers_from_bootstrap(json.loads(line)["peers"])
        except ConnectionError:
            break


threading.Thread(target=bootstrap_loop, daemon=True).start()


# ================= PYGAME =================

pygame.init()
WIDTH, HEIGHT = 700, 450
screen = pygame.display.set_mode((WIDTH, HEIGHT))
pygame.display.set_caption(f"Higher or Lower - Player {my_id}")

font = pygame.font.SysFont(None, 28)
big_font = pygame.font.SysFont(None, 40)
small_font = pygame.font.SysFont(None, 24)
clock = pygame.time.Clock()


# ================= CARD IMAGES =================

CARD_IMAGES = {}
BASE_DIR = os.path.dirname(__file__)
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




# ================= STACK / ANIMATION =================

PILE_MAX = 6
PILE_DX = 6
PILE_DY = 5
PILE_ANGLE = 2
ANIM_TIME = 0.25

# Animation state
anim = {"active": False}

# Queue for reveals so we animate exactly one card per reveal event
seen_reveals = 0                 # how many entries in game_state["revealed_cards"] we have observed
pending_reveals = []             # cards waiting to be animated


def lerp(a, b, t):
    return a + (b - a) * t


def pile_pose(i):
    """
    i: 0=bottom ... (n-1)=top within the *drawn pile*
    """
    x = WIDTH // 2 - CARD_SIZE[0] // 2 + i * PILE_DX
    y = 70 + i * PILE_DY

    # Alternate left/right tilt
    direction = -1 if i % 2 == 0 else 1
    angle = direction * (1 + i // 2) * PILE_ANGLE
    return x, y, angle


# ================= BUTTONS =================

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


# ================= MAIN LOOP =================

running = True
while running:
    for e in pygame.event.get():
        if e.type == pygame.QUIT:
            running = False
        elif e.type == pygame.MOUSEBUTTONDOWN and e.button == 1:
            if game_state.get("current_turn") == get_local_id():
                if button_higher.collidepoint(e.pos):
                    broadcast(make_event(EVENT_GUESS, {"guess": "HIGHER"}, sender=get_local_id()))
                elif button_lower.collidepoint(e.pos):
                    broadcast(make_event(EVENT_GUESS, {"guess": "LOWER"}, sender=get_local_id()))

    screen.fill((20, 20, 20))

    # Top-left info
    draw_text(f"Your ID: {get_local_id()}", 20, 10)
    draw_text(f"Leader: {get_leader_id()}", 20, 40)

    # Turn text
    turn = game_state.get("current_turn")
    if turn == get_local_id():
        draw_text_center("YOUR TURN", 40, color=(50, 220, 50))
    elif turn is not None:
        draw_text_center(f"Player {turn}'s turn", 40, color=(200, 200, 50))

    # --- Reveal queue update ---
    revealed_all = game_state.get("revealed_cards", [])

    # If game restarts / deck commit resets revealed_cards, resync queue state
    if len(revealed_all) < seen_reveals:
        seen_reveals = len(revealed_all)
        pending_reveals.clear()
        anim = {"active": False}

    # Enqueue any newly revealed cards
    if len(revealed_all) > seen_reveals:
        pending_reveals.extend(revealed_all[seen_reveals:])
        seen_reveals = len(revealed_all)

    now = pygame.time.get_ticks() / 1000.0

    # Start next animation if idle and we have pending cards
    if (not anim["active"]) and pending_reveals:
        next_card = pending_reveals.pop(0)

        # How many cards are fully "finished" (not pending, not currently animating)
        finished_count = seen_reveals - len(pending_reveals) - 1  # -1 = the one we just popped to animate

        # We only DRAW last PILE_MAX of finished, but the animated card should land on the visible top
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

    # Compute how many are finished (exclude pending and animating)
    started_not_finished = 1 if anim["active"] else 0
    finished_count = seen_reveals - len(pending_reveals) - started_not_finished
    if finished_count < 0:
        finished_count = 0

    finished_cards = revealed_all[:finished_count]
    draw_pile = finished_cards[-PILE_MAX:]

    # Draw pile
    for i, c in enumerate(draw_pile):
        img = get_card_image(c)
        if not img:
            continue
        x, y, a = pile_pose(i)
        screen.blit(pygame.transform.rotozoom(img, a, 1), (x, y))

    # Draw animated card on top
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
            f"Player {res['player']} guessed {res['guess']}",
            "CORRECT" if res["correct"] else "WRONG",
        ]

        colors = [
            (255, 255, 255),
            (220, 220, 220),
            (50, 220, 50) if res["correct"] else (220, 50, 50),
        ]

        # Draw from the top-right corner, right-aligned
        for i, (text, col) in enumerate(zip(lines, colors)):
            img = (font if i == 0 else small_font).render(text, True, col)
            x = WIDTH - margin - img.get_width()
            y = margin + i * line_h
            screen.blit(img, (x, y))


    # Buttons
    is_my_turn = (game_state.get("current_turn") == get_local_id())

    if is_my_turn:
        screen.blit(BTN_HIGHER, button_higher)
        screen.blit(BTN_LOWER, button_lower)
    else:
        # Slightly darken when disabled
        dark_higher = BTN_HIGHER.copy()
        dark_lower = BTN_LOWER.copy()
        dark_higher.fill((120, 120, 120, 255), special_flags=pygame.BLEND_RGBA_MULT)
        dark_lower.fill((120, 120, 120, 255), special_flags=pygame.BLEND_RGBA_MULT)
        screen.blit(dark_higher, button_higher)
        screen.blit(dark_lower, button_lower)


        mx, my = pygame.mouse.get_pos()
    if is_my_turn:
        if button_higher.collidepoint(mx, my):
            screen.blit(BTN_HIGHER, button_higher.move(0, -3))
        if button_lower.collidepoint(mx, my):
            screen.blit(BTN_LOWER, button_lower.move(0, -3))



    pygame.display.flip()
    clock.tick(60)

pygame.quit()
