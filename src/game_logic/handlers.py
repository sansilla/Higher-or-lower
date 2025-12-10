"""
Game event dispatcher and handlers.
This module ONLY handles interpreting game events and driving game turns.
"""

from .state import game_state, event_log
from .cards import build_deck, card_str
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

from p2p import broadcast, get_local_id, get_leader_id


def handle_event(event):
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
    else:
        print(f"[GAME] Unknown event {event_name} from {sender}")


# === TURN / LEADER HELPERS ===

def next_player_id():
    """
    Return the ID of the next player in turn order (wraps around).
    """
    players = game_state["players"]
    current = game_state["current_turn"]
    if not players or current not in players:
        return None
    idx = players.index(current)
    return players[(idx + 1) % len(players)]


def leader_handle_guess(payload, sender):
    """
    Called on the leader when it receives a GUESS event from the current player.
    Evaluates guess, reveals next card, sends RESULT + TURN_END + next TURN_START.
    """
    local_id = get_local_id()
    leader_id = get_leader_id()

    if leader_id != local_id:
        return  # only leader evaluates

    if sender != game_state["current_turn"]:
        print(f"[CHEAT?] Player {sender} sent GUESS outside their turn")
        return

    deck = game_state["deck"]
    revealed = game_state["revealed_cards"]
    if deck is None:
        print("[GAME] No deck available on leader.")
        return

    idx = len(revealed)  # next card index in the deck
    if idx >= len(deck):
        print("[GAME] Deck exhausted, game over.")
        # TODO: GAME_END event if you want
        return

    prev_card = game_state["current_card"]
    next_card = deck[idx]

    # Reveal next card
    game_state["current_card"] = next_card
    game_state["revealed_cards"].append(next_card)

    reveal_event = make_event(
        EVENT_DECK_REVEAL,
        {"card": next_card},
        sender=local_id,
    )
    broadcast(reveal_event)

    # Evaluate guess
    guess_val = payload.get("guess")
    prev_rank = prev_card[0]
    next_rank = next_card[0]

    correct = (
        (next_rank > prev_rank and guess_val == "HIGHER")
        or (next_rank < prev_rank and guess_val == "LOWER")
    )

    result_event = make_event(
        EVENT_RESULT,
        {
            "player": sender,
            "correct": correct,
            "prev": prev_card,
            "new": next_card,
            "guess": guess_val,
        },
        sender=local_id,
    )
    broadcast(result_event)

    # End this turn
    end_event = make_event(
        EVENT_TURN_END,
        {"player": sender},
        sender=local_id,
    )
    broadcast(end_event)

    # Start next turn (wrap around)
    next_p = next_player_id()
    if next_p is None:
        print("[GAME] No next player, cannot continue turns.")
        return

    game_state["current_turn"] = next_p

    turn_event = make_event(
        EVENT_TURN_START,
        {"player": next_p},
        sender=local_id,
    )
    broadcast(turn_event)


# === PER-EVENT HANDLERS ===

def handle_game_start(payload, sender):
    players = payload.get("players", [])
    game_state["players"] = players
    print(f"[GAME] Game started by {sender}. Players: {players}")


def handle_deck_commit(payload, sender):
    seed = payload.get("seed")
    print(f"[GAME] Deck committed by {sender} with seed {seed}")
    game_state["deck"] = build_deck(seed)
    game_state["revealed_cards"] = []


def handle_deck_reveal(payload, sender):
    card = payload.get("card")
    print(f"[GAME] Card revealed: {card_str(card)}")
    if isinstance(card, list):
        card = tuple(card)
    game_state["current_card"] = card
    game_state["revealed_cards"].append(card)


def handle_turn_start(payload, sender):
    turn_player = payload.get("player")
    print(f"[GAME] Turn start for player {turn_player}")
    game_state["current_turn"] = turn_player
    # NOTE: no input() here anymore – GUI (or terminal UI) will send GUESS.


def handle_guess(payload, sender):
    print(f"[GAME] Guess from {sender}: {payload}")
    leader_handle_guess(payload, sender)


def handle_result(payload, sender):
    player = payload.get("player")
    correct = payload.get("correct")
    prev = payload.get("prev")
    new = payload.get("new")
    guess = payload.get("guess")

    prev_str = card_str(prev)
    new_str = card_str(new)

    if correct:
        print(f"[GAME] Player {player} guessed {guess} correctly! {prev_str} -> {new_str}")
    else:
        print(f"[GAME] Player {player} guessed {guess} wrong. {prev_str} -> {new_str}")


def handle_turn_end(payload, sender):
    print(f"[GAME] Turn ended: {payload}")


def handle_player_join(payload, sender):
    pid = payload.get("player_id")
    if pid is not None and pid not in game_state["players"]:
        game_state["players"].append(pid)
    print(f"[GAME] Player joined: {pid}")


def handle_player_leave(payload, sender):
    pid = payload.get("player_id")
    if pid in game_state["players"]:
        game_state["players"].remove(pid)
    print(f"[GAME] Player left: {pid}")


def handle_new_leader(payload, sender):
    new_leader = payload.get("leader")
    print(f"[GAME] New leader announced by {sender}: {new_leader}")
