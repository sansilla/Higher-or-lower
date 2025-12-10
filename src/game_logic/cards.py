# game_logic/cards.py

import random

# Card definitions
RANKS = list(range(1, 14))   # 1–13 (Ace–King)
SUITS = ["H", "D", "C", "S"]  # hearts, diamonds, clubs, spades


def build_deck(seed: int):
    """
    Deterministically build and shuffle a deck given a seed.
    All nodes use the same seed so they get the same deck.
    """
    rng = random.Random(seed)
    deck = [(r, s) for r in RANKS for s in SUITS]
    rng.shuffle(deck)
    return deck


def card_str(card):
    """
    Pretty-print a card (rank + suit).
    Accepts tuple or list like (7, "H").
    """
    if isinstance(card, list):
        card = tuple(card)
    rank, suit = card
    rank_map = {1: "A", 11: "J", 12: "Q", 13: "K"}
    r = rank_map.get(rank, str(rank))
    return f"{r}{suit}"
