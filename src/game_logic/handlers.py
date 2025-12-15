from .state import game_state, event_log
from .cards import build_deck, card_str
from .persistent_log import log_event
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
    EVENT_STATE_REQUEST,
    EVENT_STATE_SNAPSHOT,
)

from p2p import broadcast, send_to, get_local_id, get_leader_id, get_player_ids



seen_events = set()


def _dedup_key(event):
    return (event.get("from"), event.get("id"))


def handle_event(event):
    key = _dedup_key(event)
    if key in seen_events:
        return
    seen_events.add(key)

    event_log.append(event)

    
    log_event(event)





    name = event["event_name"]
    payload = event["payload"]
    sender = event["from"]

    if name == EVENT_GAME_START:
        handle_game_start(payload)
    elif name == EVENT_DECK_COMMIT:
        handle_deck_commit(payload)
    elif name == EVENT_DECK_REVEAL:
        handle_deck_reveal(payload)
    elif name == EVENT_TURN_START:
        handle_turn_start(payload)
    elif name == EVENT_GUESS:
        handle_guess(payload, sender)
    elif name == EVENT_RESULT:
        pass
    elif name == EVENT_TURN_END:
        pass
    elif name == EVENT_PLAYER_JOIN:
        handle_player_join(payload)
    elif name == EVENT_PLAYER_LEAVE:
        handle_player_leave(payload)
    elif name == EVENT_NEW_LEADER:
        handle_new_leader()
    elif name == EVENT_STATE_REQUEST:
        handle_state_request(sender)
    elif name == EVENT_STATE_SNAPSHOT:
        handle_state_snapshot(payload)


# leader helpers

def _leader_only():
    return get_local_id() == get_leader_id()



def _leader_reconcile_membership():
    
    

    if not _leader_only():
        return

    alive = set(get_player_ids())
    players = set(game_state.get("players", []))

    for pid in players - alive:
        game_state["players"].remove(pid)
        broadcast(make_event(EVENT_PLAYER_LEAVE, {"player_id": pid}, sender=get_local_id()))

    for pid in alive - players:
        game_state["players"].append(pid)
        broadcast(make_event(EVENT_PLAYER_JOIN, {"player_id": pid}, sender=get_local_id()))

    game_state["players"].sort()





def _leader_advance_turn(from_pid):
    
    



    players = game_state.get("players", [])
    if not players:
        game_state["current_turn"] = None
        return



    if from_pid not in players:
        nxt = players[0]
    else:
        i = players.index(from_pid)
        nxt = players[(i + 1) % len(players)]

    game_state["current_turn"] = nxt
    broadcast(make_event(EVENT_TURN_START, {"player": nxt}, sender=get_local_id()))





#hendlers for events

def handle_game_start(payload):
    game_state["players"] = sorted(payload["players"])


def handle_deck_commit(payload):
    seed = payload["seed"]

    
    game_state["deck_seed"] = seed
    game_state["deck"] = build_deck(seed)
    game_state["revealed_cards"] = []
    game_state["current_card"] = None


def handle_deck_reveal(payload):
    card = tuple(payload["card"])
    if game_state["revealed_cards"] and game_state["revealed_cards"][-1] == card:
        return
    print(f"[GAME] Card revealed: {card_str(card)}")
    game_state["revealed_cards"].append(card)
    game_state["current_card"] = card




def handle_turn_start(payload):
  
    game_state["current_turn"] = payload["player"]
    print(f"[GAME] Turn start for player {payload['player']}")




def handle_guess(payload, sender):
    if not _leader_only():
        return

    if sender != game_state.get("current_turn"):
        return

    _leader_reconcile_membership()

    deck = game_state["deck"]
    idx = len(game_state["revealed_cards"])
    if idx >= len(deck):
        return

    prev = game_state["current_card"]
    next_card = deck[idx]

    game_state["revealed_cards"].append(next_card)
    game_state["current_card"] = next_card

    broadcast(make_event(EVENT_DECK_REVEAL, {"card": next_card}, sender=get_local_id()))

    correct = (
        (next_card[0] > prev[0] and payload["guess"] == "HIGHER") or
        (next_card[0] < prev[0] and payload["guess"] == "LOWER")
    )

    broadcast(make_event(
        EVENT_RESULT,
        {"player": sender, "correct": correct, "prev": prev, "new": next_card, "guess": payload["guess"]},
        sender=get_local_id()
    ))

    _leader_advance_turn(sender)





def handle_player_join(payload):
    pid = payload["player_id"]
    if pid not in game_state["players"]:
        game_state["players"].append(pid)
        game_state["players"].sort()


def handle_player_leave(payload):
    pid = payload["player_id"]
    if pid in game_state["players"]:
        game_state["players"].remove(pid)

    if _leader_only() and game_state.get("current_turn") == pid:
        _leader_advance_turn(pid)


def handle_new_leader():
    if not _leader_only():
        return

    _leader_reconcile_membership()

    if game_state.get("current_turn") not in game_state.get("players", []):
        _leader_advance_turn(game_state["players"][0])


#snapshot!

def handle_state_request(sender):
    if not _leader_only():
        return

    _leader_reconcile_membership()

    snap = {
        "players": list(game_state["players"]),
        "deck_seed": game_state["deck_seed"],
        "revealed_n": len(game_state["revealed_cards"]),
        "current_turn": game_state["current_turn"],
        "current_card": game_state["current_card"],
    }

    send_to(sender, make_event(EVENT_STATE_SNAPSHOT, {"snapshot": snap}, sender=get_local_id()))


def handle_state_snapshot(payload):
    snap = payload["snapshot"]

    game_state["players"] = list(snap["players"])
    game_state["deck_seed"] = snap["deck_seed"]
    game_state["deck"] = build_deck(snap["deck_seed"])
    game_state["revealed_cards"] = game_state["deck"][:snap["revealed_n"]]
    game_state["current_card"] = snap["current_card"]
    game_state["current_turn"] = snap["current_turn"]
