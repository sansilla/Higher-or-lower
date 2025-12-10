"""
Shared game state and event log for the Higher/Lower game.
This is imported by client.py and handlers.py.
"""

event_log = []  # list of events

game_state = {
    "players": [],
    "deck": None,
    "current_card": None,
    "current_turn": None,
    "revealed_cards": [],
}
