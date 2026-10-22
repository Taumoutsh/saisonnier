# saisonnier

Passerelle Rust : reçoit des requêtes JSON d'un client TCP, exécute pour chacune
une séquence d'actions (WebSocket, pauses, autres interfaces...), puis renvoie une
**response** (`DONE` / `FAILED`) et, le cas échéant, des **events**. Des messages
spontanés reçus en WebSocket peuvent aussi déclencher des séquences qui se
terminent par l'envoi d'un event.

```
                 requêtes JSON (NDJSON)                         actions WebSocket
 client_1  ───────────────────────────►  ┌──────────────┐  ───────────────────────►  équipement
 (TCP)     ◄───────────────────────────  │  saisonnier  │  ◄───────────────────────  (ws_simulator)
            responses / events           │              │   retours + notifications
                                         │  FIFO 1 à 1  │
                                         └──────┬───────┘
                          responses / events    │    autres actions (ex. accord maintenance)
                                                ▼
                                   server_1, server_2 (TCP sortants)
```

## Démarrage rapide

```bash
pip install -r tools/requirements.txt
python3 tools/ws_simulator.py                       # terminal 1 : équipement simulé (ws://0.0.0.0:9001)
cargo run --release                                 # terminal 2 : l'application (TCP :7000)
python3 tools/send_request.py turnOnDevice deviceId=lamp-1   # terminal 3 : client_1
```

```
-> {"request": "turnOnDevice", "id": "r1", "parameters": {"deviceId": "lamp-1"}}
<- {"response": "turnOnDevice", "id": "r1", "status": "DONE", "data": {"deviceId": "lamp-1", "state": "ON", "alreadyInState": false}, "timestamp": ...}
<- {"event": "deviceStateChanged", "source": "onStateChanged", "data": {"deviceId": "lamp-1", "state": "ON", "level": 0}, "timestamp": ...}
```

Dans le terminal du simulateur, on manipule l'équipement à chaud :

| commande                  | effet côté client                                        |
|---------------------------|----------------------------------------------------------|
| `fail powerOn`            | `turnOnDevice` → `FAILED` / `STEP_FAILED`, step `power`  |
| `silent getState`         | → `FAILED` / `TIMEOUT` après `STEP_TIMEOUT_MS`           |
| `delay powerOn 3`         | réponse retardée (→ `TIMEOUT` si > timeout)              |
| `ok all`                  | comportement normal                                      |
| `alarm lamp-1 overheat`   | event spontané `deviceAlarm`                             |
| `notify lamp-1 OFF`       | event spontané `deviceStateChanged`                      |

Autres exemples de client :

```bash
python3 tools/send_request.py --raw '{"request": "turnOnDevice"}'          # forme minimale
python3 tools/send_request.py setDeviceLevel deviceId=lamp-1 level=42
python3 tools/send_request.py rebootDevice deviceId=lamp-1
python3 tools/send_request.py turnOffDevice --id dup --repeat 2             # 2e envoi -> DUPLICATE
python3 tools/send_request.py -i                                            # mode interactif
```

Serveurs TCP sortants simulés : `python3 tools/tcp_server_sim.py --port 7001 --name server_1`
(`--ack` pour qu'il réponde aux demandes, ex. l'étape `rebootDevice.maintenance_ack`).

### Docker

Image finale `debian:13-slim` (trixie) sans paquet supplémentaire : le binaire ne
dépend que de la glibc (TLS en rustls, racines de certificats embarquées).

```bash
docker build -t saisonnier .
docker run --rm -p 7000:7000 -e WS_ENDPOINTS=device=ws://host.docker.internal:9001 saisonnier

# ou tout l'environnement de démo (application + simulateurs) :
docker compose up --build
docker attach saisonnier-device-sim-1     # pour taper les commandes du simulateur
```

## Protocole

Un objet JSON par ligne (NDJSON), dans les deux sens. Les messages sont validés
par les schémas de [`schemas/`](schemas/) (JSON Schema 2020-12, chargés au
démarrage depuis `SCHEMA_DIR`, les `$ref` entre fichiers sont résolus dans ce
répertoire) : les requêtes en entrée (rejet `INVALID_REQUEST` si non conformes),
les réponses et events en sortie (erreur journalisée si non conformes).

| requête           | paramètres                            | actions WebSocket par défaut                                   |
|-------------------|---------------------------------------|----------------------------------------------------------------|
| `turnOnDevice`    | `deviceId?`                           | `read_state` → `power` (si pas déjà ON) → `settle` → `verify_state` (→ `notify`*) |
| `turnOffDevice`   | `deviceId?`                           | idem vers OFF                                                  |
| `getDeviceStatus` | `deviceId?`                           | `read_state`                                                   |
| `setDeviceLevel`  | `deviceId?`, `level` (0..100)         | `set_level` → `verify_level`                                   |
| `rebootDevice`    | `deviceId?`                           | (`maintenance_ack`* sur server_2 →) `reboot` → `wait_ready` (notification) → `settle` → `verify_state` (→ `notify`*) |

\* étape optionnelle, activée par `ENABLED_STEPS`. `deviceId` absent → `DEFAULT_DEVICE_ID`.
`id` absent → généré (`auto-<n>`) et renvoyé dans la réponse.

Déclencheurs (messages spontanés de l'équipement) :

| déclencheur      | message WebSocket                               | séquence                         |
|------------------|-------------------------------------------------|----------------------------------|
| `onStateChanged` | `{"type":"notification","notification":"stateChanged",...}` | `refresh` (getState) → event `deviceStateChanged` |
| `onAlarm`        | `{"type":"notification","notification":"alarm",...}`        | event `deviceAlarm`              |

Réponse en échec :

```json
{"response": "turnOnDevice", "id": "r1", "status": "FAILED",
 "error": {"code": "TIMEOUT", "message": "pas de réponse de « device » : timeout de 2000 ms", "step": "verify_state"},
 "timestamp": 1792696359902}
```

Codes : `INVALID_REQUEST`, `DUPLICATE`, `QUEUE_FULL`, `TIMEOUT`, `STEP_FAILED`,
`INTERFACE_UNAVAILABLE`, `INTERNAL`.

Protocole WebSocket avec l'équipement (voir `src/workflows/device.rs`) :
commande `{"id", "action", "deviceId", ...}`, retour corrélé par `id`
`{"id", "status": "OK"|"ERROR", "data", "error"}`, notifications
`{"type": "notification", "notification": ..., "deviceId", ...}`.

## Fonctionnement

- **FIFO** : toutes les requêtes (et séquences déclenchées) passent par une file
  unique traitée une à une. Une requête est rejetée (`DUPLICATE`) si une requête
  de même `id`, ou de même nom + mêmes paramètres, est déjà en attente ou en cours
  (`DEDUP_MODE`). File pleine → `QUEUE_FULL`.
- **Timeouts** : chaque requête a un timeout global (`TIMEOUT_<NOM>_MS`, sinon
  `DEFAULT_TIMEOUT_MS`), chaque attente de message un timeout d'étape
  (`STEP_TIMEOUT_<NOM>_<ETAPE>_MS`, sinon valeur du code, sinon `STEP_TIMEOUT_MS`).
  Dépassement → `FAILED` / `TIMEOUT` avec l'étape en cours.
- **Interfaces** : toutes les connexions externes (WebSocket, serveurs TCP
  sortants) sont des `Interface` nommées, avec reconnexion automatique. Une
  action désigne son interface par son nom ; une interface non connectée fait
  échouer immédiatement l'étape (`INTERFACE_UNAVAILABLE`).
- **Boîte aux lettres** : chaque workflow est abonné à toutes les interfaces dès
  son démarrage, si bien qu'une réponse ou notification arrivée avant l'étape qui
  l'attend n'est pas perdue.

### Types d'actions (`src/engine/workflow.rs`)

| action                        | rôle                                                              |
|-------------------------------|-------------------------------------------------------------------|
| `Action::request(iface, f)`   | envoie un message (+ `id` de corrélation), attend le retour       |
| `Action::send(iface, f)`      | envoie sans attendre                                              |
| `Action::wait_for(iface, f)`  | attend un message spontané correspondant au filtre                |
| `Action::pause_ms(ms)`        | pause (surchargeable par `PAUSE_<WF>_<ETAPE>_MS`)                 |
| `Action::emit(event, f)`      | émet un event vers `EVENT_TARGETS`                                |
| `Action::local(f)`            | traitement local sur le contexte                                  |

Chaque `Step` peut porter : `.check(..)` (contrôle du message reçu),
`.store_as("var")` (stockage dans le contexte, relu par `ctx.get("/var/...")`),
`.when(..)` (condition), `.timeout_ms(..)`, `.continue_on_error()`.

### Ajouter / retirer des actions

Dans `src/workflows/device.rs`, un workflow est une simple liste :

```rust
WorkflowBuilder::new(name, s)
    .then(Step::new("read_state", call(&iface, "getState", dev, json!({}))).check(expect_ok).store_as("initial"))
    .then(Step::new("settle", Action::pause_ms(0)))
    .then_optional(Step::new("notify", Action::emit("deviceTurnedOn", |_| json!({...}))))
    .result(|ctx| json!({...}))
    .build()
```

- `.then(...)` : étape active par défaut, retirable par `DISABLED_STEPS`.
- `.then_optional(...)` : étape inactive par défaut, ajoutée par `ENABLED_STEPS`.

**Nouvelle requête** : ajouter son nom dans `schemas/common.schema.json`
(+ ses paramètres dans `request.schema.json`), une variante dans `Command`
(`src/model.rs`), et son workflow dans `workflows::plan`.
**Nouvelle interface** (MQTT, HTTP, série...) : écrire un driver dans
`src/interfaces/` sur le modèle de `websocket.rs` / `tcp.rs`.

## Variables d'environnement

| variable                          | défaut                         | rôle |
|-----------------------------------|--------------------------------|------|
| `LISTEN_ADDR`                     | `0.0.0.0:7000`                 | écoute TCP du client |
| `WS_ENDPOINTS`                    | `device=ws://127.0.0.1:9001`   | interfaces WebSocket `nom=url,...` |
| `TCP_SERVERS`                     | —                              | serveurs TCP sortants `nom=host:port,...` (ex. `server_1=10.0.0.1:7001`) |
| `RESPONSE_TARGETS`                | `origin`                       | destinations des réponses : `origin`, `clients`, nom de serveur |
| `EVENT_TARGETS`                   | `clients`                      | destinations des events : `clients`, nom de serveur |
| `SCHEMA_DIR`                      | `schemas`                      | répertoire des schémas JSON |
| `VALIDATE_OUTPUT`                 | `true`                         | validation des messages sortants |
| `DEFAULT_TIMEOUT_MS`              | `5000`                         | timeout global d'une requête |
| `TIMEOUT_<NOM>_MS`                | —                              | ex. `TIMEOUT_TURN_ON_DEVICE_MS=3000` |
| `STEP_TIMEOUT_MS`                 | `2000`                         | timeout d'attente d'un message |
| `STEP_TIMEOUT_<NOM>_<ETAPE>_MS`   | —                              | ex. `STEP_TIMEOUT_REBOOT_DEVICE_WAIT_READY_MS=20000` |
| `PAUSE_<NOM>_<ETAPE>_MS`          | —                              | ex. `PAUSE_TURN_ON_DEVICE_SETTLE_MS=500` |
| `DISABLED_STEPS`                  | —                              | ex. `turnOnDevice.verify_state,*.read_state` |
| `ENABLED_STEPS`                   | —                              | ex. `turnOnDevice.notify,rebootDevice.maintenance_ack` |
| `DISABLED_TRIGGERS`               | —                              | ex. `onStateChanged` |
| `DEVICE_INTERFACE`                | `device`                       | interface utilisée pour l'équipement |
| `MAINTENANCE_INTERFACE`           | `server_2`                     | interface de l'étape `maintenance_ack` |
| `DEFAULT_DEVICE_ID`               | `device-1`                     | `deviceId` si absent de la requête |
| `QUEUE_CAPACITY`                  | `100`                          | taille de la FIFO |
| `DEDUP_MODE`                      | `both`                         | `off`, `id`, `content`, `both` |
| `RECONNECT_DELAY_MS`              | `1000`                         | délai de reconnexion des interfaces |
| `RUST_LOG`                        | `info`                         | niveau de logs (`debug` pour voir tous les messages) |

`<NOM>` / `<ETAPE>` : nom en MAJUSCULES_SNAKE (`turnOnDevice` → `TURN_ON_DEVICE`).

## Tests

```bash
cargo test     # unitaires + bout-en-bout (faux équipement WebSocket en Rust)
```

## Organisation du code

```
src/
  main.rs               démarrage, signaux d'arrêt
  lib.rs                assemblage (interfaces, moteur, serveur)
  config.rs             variables d'environnement
  model.rs              modèle d'entrée (Command typée) et de sortie (Response, Event)
  schema.rs             validation JSON Schema
  server.rs             serveur TCP d'entrée (client_1)
  router.rs             routage des réponses / events
  engine/mod.rs         FIFO, doublons, déclencheurs
  engine/workflow.rs    Workflow, Step, Action, Ctx
  engine/executor.rs    exécution des étapes, timeouts, corrélation
  interfaces/           Interface générique + drivers WebSocket et TCP
  workflows/            catalogue requête -> actions, déclencheurs
schemas/                modèles JSON
tools/                  client, simulateur WebSocket, simulateur serveur TCP
```
