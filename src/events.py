import time
import json

EVENT_GAME_START = "GAME_START"
EVENT_DECK_COMMIT = "DECK_COMMIT" #lock the order of the deck
EVENT_DECK_REVEAL = "DECK_REVEAL"

EVENT_TURN_START = "TURN_START"
EVENT_GUESS = "GUESS"
EVENT_RESULT = "RESULT"
EVENT_TURN_END = "TURN_END"

EVENT_PLAYER_JOIN = "PLAYER_JOIN"
EVENT_PLAYER_LEAVE = "PLAYER_LEAVE"
EVENT_NEW_LEADER = "NEW_LEADER"

current_event_id = 0

def make_event(event_name, payload, sender):
    """
    Constructs a standard event. event_name: str (e.g. TURN_START),
    payload: dict (event data), sender: peer ID
    """
    global current_event_id

    event = {
        "id": current_event_id,
        "event_name": event_name,
        "payload": payload,
        "from": sender,
        "timestamp": time.time()
    }

    current_event_id += 1
    return event
