

import random





RANKS = list(range(1, 14))
SUITS = ["H", "D", "C", "S"]  # hearts, diamonds, clubs, spades!!!!


def build_deck(seed: int):

    rng = random.Random(seed)
    deck = [(r, s) for r in RANKS for s in SUITS]



    rng.shuffle(deck)
    return deck


def card_str(card):

    if isinstance(card, list):
        card = tuple(card)
    rank, suit = card


    rank_map = {1: "A", 11: "J", 12: "Q", 13: "K"}
    
    r = rank_map.get(rank, str(rank))
    return f"{r}{suit}"
