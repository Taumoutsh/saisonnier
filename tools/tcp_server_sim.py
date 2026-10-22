#!/usr/bin/env python3
"""Simulateur de serveur TCP sortant (serveur_1, serveur_2, ...).

L'application s'y connecte (variable TCP_SERVERS) et y pousse réponses et
évènements (RESPONSE_TARGETS / EVENT_TARGETS), un JSON par ligne.

Avec --ack, il répond aux demandes portant un « id » (ex. étape optionnelle
rebootDevice.maintenance_ack) par {"id": ..., "status": "OK"} — ou ERROR avec
--refuse.

  ./tcp_server_sim.py --port 7001 --name server_1
  ./tcp_server_sim.py --port 7002 --name server_2 --ack
"""

import argparse
import asyncio
import json
import time

args = None


def log(direction: str, msg) -> None:
    text = msg if isinstance(msg, str) else json.dumps(msg, ensure_ascii=False)
    print(f"{time.strftime('%H:%M:%S')} [{args.name}] {direction} {text}", flush=True)


async def connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    log("**", f"application connectée {writer.get_extra_info('peername')}")
    while line := await reader.readline():
        if not line.strip():
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            log("!!", f"ligne non JSON : {line!r}")
            continue
        log("<-", msg)
        is_demand = "id" in msg and "response" not in msg and "event" not in msg
        if args.ack and is_demand:
            reply = {"id": msg["id"], "status": "ERROR" if args.refuse else "OK"}
            if args.refuse:
                reply["error"] = "refused by operator (simulated)"
            await asyncio.sleep(args.ack_delay)
            log("->", reply)
            writer.write((json.dumps(reply) + "\n").encode())
            await writer.drain()
    log("**", "application déconnectée")


async def main() -> None:
    global args
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=7001)
    ap.add_argument("--name", default="server_1")
    ap.add_argument("--ack", action="store_true", help="répondre aux demandes portant un id")
    ap.add_argument("--refuse", action="store_true", help="répondre ERROR au lieu de OK")
    ap.add_argument("--ack-delay", type=float, default=0.2)
    args = ap.parse_args()
    server = await asyncio.start_server(connection, args.host, args.port)
    log("**", f"en écoute sur {args.host}:{args.port}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
