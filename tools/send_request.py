#!/usr/bin/env python3
"""Client TCP de test (client_1) : envoie des requêtes JSON et affiche les
réponses / évènements renvoyés par l'application.

Protocole : un objet JSON par ligne (NDJSON), dans les deux sens.

Exemples :
  # requête simple, attend la réponse puis quitte
  ./send_request.py turnOnDevice
  ./send_request.py turnOnDevice deviceId=lamp-1
  ./send_request.py setDeviceLevel deviceId=lamp-1 level=42 --id req-42

  # JSON brut
  ./send_request.py --raw '{"request": "rebootDevice", "parameters": {"deviceId": "lamp-1"}}'

  # même requête envoyée 2 fois d'affilée (test des doublons)
  ./send_request.py turnOnDevice --id dup --repeat 2

  # mode interactif : une ligne = une requête (« nom k=v ... » ou JSON brut),
  # les évènements spontanés sont affichés au fil de l'eau
  ./send_request.py -i
"""

import argparse
import json
import socket
import sys
import threading
import time
import uuid

COLORS = {"DONE": "\033[32m", "FAILED": "\033[31m", "event": "\033[36m", "reset": "\033[0m"}


def parse_value(raw: str):
    """`42` -> 42, `true` -> True, `"x"` -> "x", sinon chaîne brute."""
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def build_request(name: str, params: list[str], req_id: str | None, auto_id: bool) -> dict:
    msg = {"request": name}
    if req_id:
        msg["id"] = req_id
    elif auto_id:
        msg["id"] = uuid.uuid4().hex[:8]
    if params:
        msg["parameters"] = {}
        for p in params:
            key, sep, value = p.partition("=")
            if not sep:
                raise SystemExit(f"paramètre invalide « {p} », format attendu clé=valeur")
            msg["parameters"][key] = parse_value(value)
    return msg


def show(direction: str, msg: dict, color: bool) -> None:
    text = json.dumps(msg, ensure_ascii=False)
    key = msg.get("status") or ("event" if "event" in msg else None)
    if color and key in COLORS:
        text = f"{COLORS[key]}{text}{COLORS['reset']}"
    print(f"{time.strftime('%H:%M:%S')} {direction} {text}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("name", nargs="?", help="nom de la requête (ex. turnOnDevice)")
    ap.add_argument("params", nargs="*", help="paramètres clé=valeur")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=7000)
    ap.add_argument("--id", help="identifiant de la requête (défaut : aléatoire)")
    ap.add_argument("--no-id", action="store_true", help="ne pas envoyer d'id (l'application en génère un)")
    ap.add_argument("--raw", help="message JSON brut à envoyer")
    ap.add_argument("--repeat", type=int, default=1, help="nombre d'envois")
    ap.add_argument("--wait", type=float, default=30.0, help="attente max des réponses (s)")
    ap.add_argument("--listen", type=float, default=0.5, help="écoute des évènements après la dernière réponse (s)")
    ap.add_argument("-i", "--interactive", action="store_true")
    ap.add_argument("--no-color", action="store_true")
    args = ap.parse_args()
    color = not args.no_color and sys.stdout.isatty()

    sock = socket.create_connection((args.host, args.port), timeout=5)
    sock.settimeout(None)
    pending = 0
    lock = threading.Condition()

    def reader():
        nonlocal pending
        buf = b""
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                print("connexion fermée par l'application", file=sys.stderr)
                with lock:
                    pending = -1
                    lock.notify_all()
                return
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if not line.strip():
                    continue
                msg = json.loads(line)
                show("<-", msg, color)
                if "response" in msg:
                    with lock:
                        pending -= 1
                        lock.notify_all()

    threading.Thread(target=reader, daemon=True).start()

    def send(msg: dict) -> None:
        nonlocal pending
        with lock:
            pending += 1
        show("->", msg, color)
        sock.sendall((json.dumps(msg) + "\n").encode())

    if args.interactive:
        print("mode interactif : « nom clé=valeur ... » ou JSON brut, Ctrl-D pour quitter", file=sys.stderr)
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                if line.startswith("{"):
                    send(json.loads(line))
                else:
                    name, *params = line.split()
                    send(build_request(name, params, None, True))
            except (json.JSONDecodeError, SystemExit) as e:
                print(f"entrée invalide : {e}", file=sys.stderr)
        return 0

    if args.raw:
        msg = json.loads(args.raw)
    elif args.name:
        msg = build_request(args.name, args.params, args.id, not args.no_id)
    else:
        ap.error("indiquer un nom de requête, --raw ou -i")

    for _ in range(args.repeat):
        send(msg)

    with lock:
        lock.wait_for(lambda: pending <= 0, timeout=args.wait)
        if pending > 0:
            print(f"{pending} réponse(s) non reçue(s) après {args.wait}s", file=sys.stderr)
            return 1
    time.sleep(args.listen)  # laisse arriver les évènements consécutifs
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
