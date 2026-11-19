# server.py  -  HORSE CASINO server
# Run it with:  python server.py
#
# WHY TCP AND UDP?
#   TCP  = reliable (nothing gets lost, correct order). We use it for things that
#          MUST arrive: login, rooms, bets, race results, scores.
#   UDP  = fast but a packet can get lost. We use it for things that are sent
#          all the time and where a lost packet doesn't matter:
#          horse positions (20 per second) and the "cheer" button presses.

import socket
import threading
import json
import time
import random

HOST = "0.0.0.0"        # listen on every network card of this computer
TCP_PORT = 5000
UDP_PORT = 5001

START_CHIPS = 100       # chips every player gets when a game starts
ROUNDS = 3              # rounds per game
NUM_HORSES = 4
TRACK_LENGTH = 1000.0   # when a horse reaches this, the race is over
BET_TIME = 15           # seconds players have to place a bet
MIN_BET = 10
PAYOUT = 3              # winning bet pays amount * 3
MAX_PLAYERS = 3         # players per room
TICK = 0.05             # one race update every 0.05 s = 20 per second
CHEER_BOOST = 4.0       # how far a cheer pushes the horse
CHEER_COOLDOWN = 0.15   # seconds between two cheers of the same player

lock = threading.RLock()   # one lock so threads don't mess up shared data
accounts = {}              # name -> number of games won (our "registration")
players = {}               # name -> {"conn": tcp socket, "udp": address, "room": room name}
rooms = {}                 # room name -> room dictionary

udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


# ---------------------------------------------------------------- sending helpers
def send_raw(conn, msg):
    """Send one JSON message over TCP. One message = one line."""
    with lock:
        try:
            conn.sendall((json.dumps(msg) + "\n").encode())
        except OSError:
            pass   # dead socket: the thread that reads it will clean up


def send(name, msg):
    """Send a message to a player by name."""
    if name in players:
        send_raw(players[name]["conn"], msg)


def broadcast(room, msg):
    """Send a message to everybody in a room."""
    for name in list(room["members"]):
        send(name, msg)


def room_info(room):
    """The room status that clients draw on screen."""
    return {"type": "room_update", "name": room["name"], "host": room["host"],
            "members": room["members"], "chips": room["chips"],
            "state": room["state"], "bet_names": list(room["bets"])}


def send_rooms(name):
    """Send the list of rooms to one player (for the lobby screen)."""
    lst = [{"name": r["name"], "count": len(r["members"]), "state": r["state"]}
           for r in rooms.values()]
    send(name, {"type": "rooms", "rooms": lst})


def push_lobby():
    """Update the room list of every player who is not inside a room."""
    for name, p in players.items():
        if p["room"] is None:
            send_rooms(name)


# ---------------------------------------------------------------- room helpers
def remove_from_room(name):
    """Take a player out of their room (used for leaving AND disconnecting)."""
    p = players.get(name)
    if not p or not p["room"]:
        return
    room = rooms.get(p["room"])
    p["room"] = None
    if room is None:
        return
    if name in room["members"]:
        room["members"].remove(name)
    room["bets"].pop(name, None)
    room["chips"].pop(name, None)
    if not room["members"]:
        del rooms[room["name"]]           # empty room -> delete it
    else:
        if room["host"] == name:
            room["host"] = room["members"][0]   # new host
        broadcast(room, {"type": "player_left", "name": name})
        broadcast(room, room_info(room))
    push_lobby()


def alive(room):
    """A game keeps going only while the room exists and has 2+ players."""
    return rooms.get(room["name"]) is room and len(room["members"]) >= 2


# ---------------------------------------------------------------- the game
def game_loop(room):
    """Runs in its own thread: all rounds of one game."""
    for rnd in range(1, ROUNDS + 1):
        if not run_round(room, rnd):
            break
    end_game(room)


def run_round(room, rnd):
    # ----- betting phase (clients play the calm music here)
    with lock:
        room["state"] = "betting"
        room["bets"] = {}
        broadcast(room, {"type": "betting", "round": rnd, "rounds": ROUNDS,
                         "seconds": BET_TIME})
        broadcast(room, room_info(room))
    start = time.time()
    while time.time() - start < BET_TIME:
        time.sleep(0.2)
        with lock:
            if not alive(room):
                return False
            # players who can still bet but haven't yet
            waiting = [m for m in room["members"]
                       if room["chips"].get(m, 0) >= MIN_BET and m not in room["bets"]]
            if not waiting:
                break                      # everyone bet -> start the race early

    # ----- race phase (clients play the chaos music here)
    with lock:
        if not alive(room):
            return False
        room["state"] = "racing"
        room["boost"] = [0.0] * NUM_HORSES
        broadcast(room, {"type": "race_start", "bets": room["bets"]})
    pos = [0.0] * NUM_HORSES
    speed = [4.0] * NUM_HORSES
    while max(pos) < TRACK_LENGTH:
        time.sleep(TICK)
        with lock:
            if not alive(room):
                return False
            for h in range(NUM_HORSES):
                # speed changes a little every tick -> horses overtake each other
                speed[h] = min(6.0, max(2.0, speed[h] + random.uniform(-0.4, 0.4)))
                pos[h] += speed[h] + room["boost"][h]
                room["boost"][h] = 0.0
            # send positions over UDP (fast, ok if one packet is lost)
            packet = json.dumps({"type": "pos", "pos": [round(x, 1) for x in pos]}).encode()
            for m in room["members"]:
                addr = players[m]["udp"]
                if addr:
                    udp_sock.sendto(packet, addr)

    # ----- results (sent over TCP because they must arrive!)
    with lock:
        winner = pos.index(max(pos))
        order = sorted(range(NUM_HORSES), key=lambda h: -pos[h])
        payouts = {}
        for name, (horse, amount) in room["bets"].items():
            if name in room["chips"]:
                if horse == winner:
                    room["chips"][name] += amount * PAYOUT
                    payouts[name] = amount * (PAYOUT - 1)   # profit
                else:
                    payouts[name] = -amount                  # lost the bet
        room["state"] = "results"
        broadcast(room, {"type": "race_end", "winner": winner, "order": order,
                         "pos": pos, "payouts": payouts, "chips": room["chips"]})
    time.sleep(6)   # let players look at the results
    return True


def end_game(room):
    """Final ranking, winner, back to the waiting room."""
    with lock:
        if rooms.get(room["name"]) is not room or not room["members"]:
            return
        ranking = sorted(room["chips"].items(), key=lambda kv: -kv[1])
        winners = []
        if ranking:
            best = ranking[0][1]
            winners = [n for n, c in ranking if c == best]
            for n in winners:
                accounts[n] += 1
        broadcast(room, {"type": "game_end", "ranking": ranking, "winners": winners,
                         "wins": accounts})
        room["state"] = "waiting"
        room["bets"] = {}
        broadcast(room, room_info(room))
        push_lobby()


# ---------------------------------------------------------------- TCP: one thread per client
def process(conn, name, msg):
    """Handle one message from a client. Returns the player's name."""
    kind = msg.get("type")
    with lock:
        # ----- LOGIN / REGISTRATION
        if kind == "login":
            new = str(msg.get("name", "")).strip()[:12]
            if name:
                return name                         # already logged in
            if not new:
                send_raw(conn, {"type": "error", "msg": "Name is empty"})
            elif new in players:
                send_raw(conn, {"type": "error", "msg": "That name is already online"})
            else:
                players[new] = {"conn": conn, "udp": None, "room": None}
                accounts.setdefault(new, 0)         # first time = registered
                print("Login:", new)
                send_raw(conn, {"type": "login_ok", "name": new, "wins": accounts[new]})
                send_rooms(new)
                return new
            return name

        if name is None:
            send_raw(conn, {"type": "error", "msg": "Login first"})
            return name

        room = rooms.get(players[name]["room"])      # the room I'm in (or None)

        if kind == "ping":
            pass                                     # just keeps the connection alive

        elif kind == "list_rooms":
            send_rooms(name)

        # ----- CREATE ROOM
        elif kind == "create_room":
            rname = str(msg.get("room", "")).strip()[:20]
            if room:
                send(name, {"type": "error", "msg": "You are already in a room"})
            elif not rname or rname in rooms:
                send(name, {"type": "error", "msg": "Room name empty or taken"})
            else:
                new_room = {"name": rname, "host": name, "members": [name],
                            "state": "waiting", "bets": {}, "chips": {},
                            "boost": [0.0] * NUM_HORSES, "last_cheer": {}}
                rooms[rname] = new_room
                players[name]["room"] = rname
                send(name, room_info(new_room))
                push_lobby()

        # ----- JOIN ROOM
        elif kind == "join_room":
            target = rooms.get(msg.get("room"))
            if room or target is None:
                send(name, {"type": "error", "msg": "Can't join that room"})
            elif target["state"] != "waiting":
                send(name, {"type": "error", "msg": "Game already running"})
            elif len(target["members"]) >= MAX_PLAYERS:
                send(name, {"type": "error", "msg": "Room is full"})
            else:
                target["members"].append(name)
                players[name]["room"] = target["name"]
                broadcast(target, room_info(target))
                push_lobby()

        # ----- LEAVE ROOM
        elif kind == "leave_room":
            remove_from_room(name)
            send(name, {"type": "left_room"})
            send_rooms(name)

        # ----- START GAME (host only)
        elif kind == "start_game":
            if not room or room["host"] != name:
                send(name, {"type": "error", "msg": "Only the host can start"})
            elif room["state"] != "waiting":
                send(name, {"type": "error", "msg": "Game already running"})
            elif len(room["members"]) < 2:
                send(name, {"type": "error", "msg": "Need at least 2 players"})
            else:
                room["chips"] = {m: START_CHIPS for m in room["members"]}
                print("Game started in room:", room["name"])
                threading.Thread(target=game_loop, args=(room,), daemon=True).start()
                push_lobby()

        # ----- PLACE A BET
        elif kind == "bet":
            if room and room["state"] == "betting" and name not in room["bets"]:
                horse = int(msg.get("horse", -1))
                amount = int(msg.get("amount", 0))
                if 0 <= horse < NUM_HORSES and MIN_BET <= amount <= room["chips"].get(name, 0):
                    room["bets"][name] = (horse, amount)
                    room["chips"][name] -= amount     # the bet leaves your pocket
                    send(name, {"type": "bet_ok", "horse": horse, "amount": amount})
                    broadcast(room, room_info(room))
                else:
                    send(name, {"type": "error", "msg": "Bad bet"})
    return name


def disconnect(name, conn):
    """Called when a client's TCP connection dies (closed, crashed, wifi lost...)."""
    with lock:
        if name and name in players and players[name]["conn"] is conn:
            print("Disconnected:", name)
            remove_from_room(name)        # the others get "player_left"
            del players[name]
            push_lobby()


def handle_client(conn, addr):
    print("New TCP connection from", addr)
    conn.settimeout(15)   # clients ping every 2 s. 15 s of silence = dead client
    name = None
    buf = ""
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break                      # client closed the connection
            buf += data.decode()
            while "\n" in buf:             # TCP is a stream, so we split by lines
                line, buf = buf.split("\n", 1)
                if line.strip():
                    name = process(conn, name, json.loads(line))
    except Exception:
        pass                               # timeout / reset / bad data = disconnect
    finally:
        disconnect(name, conn)
        conn.close()


# ---------------------------------------------------------------- UDP: one thread for everybody
def udp_loop():
    """Receives 'hello' (so we learn the player's UDP address) and 'cheer' packets."""
    while True:
        try:
            data, addr = udp_sock.recvfrom(1024)
            msg = json.loads(data.decode())
        except Exception:
            continue   # (Windows sometimes raises an error here after a client leaves)
        with lock:
            p = players.get(msg.get("name"))
            if not p:
                continue
            if msg.get("type") == "hello":
                p["udp"] = addr
            elif msg.get("type") == "cheer":
                room = rooms.get(p["room"])
                if room and room["state"] == "racing" and msg["name"] in room["bets"]:
                    now = time.time()
                    if now - room["last_cheer"].get(msg["name"], 0) >= CHEER_COOLDOWN:
                        room["last_cheer"][msg["name"]] = now
                        horse = room["bets"][msg["name"]][0]
                        room["boost"][horse] += CHEER_BOOST   # push MY horse forward


def my_ip():
    """Just to print the IP that the other computers must type."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))      # no data is sent, it only picks the right card
        ip = s.getsockname()[0]
        s.close()
        return ip
    except OSError:
        return "unknown"


def main():
    udp_sock.bind((HOST, UDP_PORT))
    threading.Thread(target=udp_loop, daemon=True).start()

    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    tcp.bind((HOST, TCP_PORT))
    tcp.listen()
    print("HORSE CASINO server running.")
    print("Clients must connect to IP:", my_ip(), " (TCP", TCP_PORT, "/ UDP", UDP_PORT, ")")

    while True:
        conn, addr = tcp.accept()          # wait for a new client
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


main()
