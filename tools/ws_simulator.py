#!/usr/bin/env python3
"""Simulateur d'équipement WebSocket (côté « serveur_1 » des actions WS).

Protocole :
  reçu    : {"id": "...", "action": "getState|powerOn|powerOff|setLevel|reboot", "deviceId": "...", ...}
  renvoyé : {"id": "...", "status": "OK", "data": {"state": "ON", "level": 0}}
            {"id": "...", "status": "ERROR", "error": "..."}
  spontané: {"type": "notification", "notification": "stateChanged|ready|alarm", "deviceId": "...", ...}

Le comportement se manipule au lancement ou à chaud, en tapant des commandes
sur l'entrée standard :

  fail <action>           l'action répond ERROR           (ex. fail powerOn)
  silent <action>         l'action ne répond plus (-> TIMEOUT côté application)
  delay <action> <sec>    l'action répond après un délai   (ex. delay powerOn 3)
  ok <action>|all         rétablit le comportement normal
  notify <device> <state> envoie un stateChanged spontané  (ex. notify lamp-1 OFF)
  alarm <device> <alarm>  envoie une alarme spontanée      (ex. alarm lamp-1 overheat)
  ready <device>          envoie une notification ready
  send <json>             envoie un message JSON arbitraire
  state                   affiche l'état des équipements
  help

Options équivalentes au lancement : --fail powerOn --silent getState --delay powerOn=2
"""

import argparse
import asyncio
import json
import sys
import time

import websockets
from websockets.asyncio.server import serve

devices: dict[str, dict] = {}
fail: set[str] = set()
silent: set[str] = set()
delays: dict[str, float] = {}
clients: set = set()
args = None


def log(direction: str, msg) -> None:
    text = msg if isinstance(msg, str) else json.dumps(msg, ensure_ascii=False)
    print(f"{time.strftime('%H:%M:%S')} {direction} {text}", flush=True)


def device(dev_id: str) -> dict:
    return devices.setdefault(dev_id, {"state": "OFF", "level": 0})


async def broadcast(msg: dict) -> None:
    log("~>", msg)
    for ws in list(clients):
        try:
            await ws.send(json.dumps(msg))
        except websockets.ConnectionClosed:
            pass


async def handle(ws, msg: dict) -> None:
    action, dev_id, corr = msg.get("action"), msg.get("deviceId", "device-1"), msg.get("id")
    if action in silent:
        log("..", f"{action} : pas de réponse (silent)")
        return
    if action in delays:
        await asyncio.sleep(delays[action])
    if action in fail:
        reply = {"id": corr, "status": "ERROR", "error": f"{action} failed (simulated)"}
        log("->", reply)
        await ws.send(json.dumps(reply))
        return

    d = device(dev_id)
    after = None
    if action == "getState":
        pass
    elif action in ("powerOn", "powerOff"):
        d["state"] = "ON" if action == "powerOn" else "OFF"
        after = {"type": "notification", "notification": "stateChanged", "deviceId": dev_id, "state": d["state"]}
    elif action == "setLevel":
        d["level"] = int(msg.get("level", 0))
    elif action == "reboot":
        d["state"] = "REBOOTING"
    else:
        reply = {"id": corr, "status": "ERROR", "error": f"unknown action {action}"}
        log("->", reply)
        await ws.send(json.dumps(reply))
        return

    reply = {"id": corr, "status": "OK", "data": dict(d)}
    log("->", reply)
    await ws.send(json.dumps(reply))

    if action == "reboot":
        await asyncio.sleep(args.reboot_time)
        d["state"] = "ON"
        after = {"type": "notification", "notification": "ready", "deviceId": dev_id}
    if after and not args.no_notifications:
        await asyncio.sleep(0.05)
        await broadcast(after)


async def connection(ws) -> None:
    clients.add(ws)
    log("**", f"application connectée ({ws.remote_address})")
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                log("!!", f"message non JSON : {raw!r}")
                continue
            log("<-", msg)
            asyncio.create_task(handle(ws, msg))
    except websockets.ConnectionClosed:
        pass
    finally:
        clients.discard(ws)
        log("**", "application déconnectée")


async def console() -> None:
    loop = asyncio.get_running_loop()
    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if not line:
            return
        parts = line.split()
        if not parts:
            continue
        cmd, rest = parts[0], parts[1:]
        try:
            if cmd == "fail":
                fail.add(rest[0])
            elif cmd == "silent":
                silent.add(rest[0])
            elif cmd == "delay":
                delays[rest[0]] = float(rest[1])
            elif cmd == "ok":
                for s in (fail, silent):
                    s.clear() if rest[0] == "all" else s.discard(rest[0])
                delays.clear() if rest[0] == "all" else delays.pop(rest[0], None)
            elif cmd == "notify":
                device(rest[0])["state"] = rest[1]
                await broadcast({"type": "notification", "notification": "stateChanged", "deviceId": rest[0], "state": rest[1]})
            elif cmd == "alarm":
                await broadcast({"type": "notification", "notification": "alarm", "deviceId": rest[0], "alarm": rest[1],
                                 "severity": rest[2] if len(rest) > 2 else "warning"})
            elif cmd == "ready":
                await broadcast({"type": "notification", "notification": "ready", "deviceId": rest[0]})
            elif cmd == "send":
                await broadcast(json.loads(line.split(None, 1)[1]))
            elif cmd == "state":
                print(json.dumps(devices, indent=2))
            else:
                print(__doc__)
                continue
            print(f"ok — fail={sorted(fail)} silent={sorted(silent)} delay={delays}", flush=True)
        except (IndexError, ValueError) as e:
            print(f"commande invalide ({e}), tapez help", flush=True)


async def main() -> None:
    global args
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9001)
    ap.add_argument("--fail", action="append", default=[], metavar="ACTION")
    ap.add_argument("--silent", action="append", default=[], metavar="ACTION")
    ap.add_argument("--delay", action="append", default=[], metavar="ACTION=SEC")
    ap.add_argument("--reboot-time", type=float, default=2.0, help="durée d'un reboot avant « ready » (s)")
    ap.add_argument("--no-notifications", action="store_true", help="pas de stateChanged automatique")
    args = ap.parse_args()
    fail.update(args.fail)
    silent.update(args.silent)
    for d in args.delay:
        action, _, sec = d.partition("=")
        delays[action] = float(sec)

    async with serve(connection, args.host, args.port):
        log("**", f"simulateur WebSocket en écoute sur ws://{args.host}:{args.port} (tapez help)")
        asyncio.create_task(console())
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
