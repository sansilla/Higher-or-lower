# Higher or Lower Game (Python)

**Higher or Lower** drinking game implemented in Python.

## Game Logic

- You need some drinks.
- The first player is shown a card.
- They guess whether the next card will be **higher** or **lower** than the current one.
- If they guess **right**, they can either:
  - Pass the turn, or
  - Make someone else drink (depending on your house rules)
- If they guess **wrong**, they drink.
- The game continues with each player taking turns.

## How to Play / Test

You can test the connection by running the following commands in separate terminals:

**1st terminal main node (bootstrap):**

```bash
python3 server.py
```

**other terminals (clients). Instead of "username" insert your username:**

```bash
python3 pygame_client.py "username"
```


IMPORTANT!!!
If running on multiple devices, set BOOTSTRAP_HOST in pygame_client.py to the server machine’s LAN IP.