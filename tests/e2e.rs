//! Tests bout-en-bout : client TCP -> application -> faux équipement WebSocket.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

use saisonnier::config::Settings;

#[derive(Default)]
struct DeviceState {
    devices: HashMap<String, (String, u64)>,
    fail: HashSet<String>,
    silent: HashSet<String>,
    delay_ms: HashMap<String, u64>,
    calls: Vec<String>,
}

/// Faux équipement WebSocket, pilotable depuis le test.
#[derive(Clone)]
struct MockDevice {
    url: String,
    state: Arc<Mutex<DeviceState>>,
    push: broadcast::Sender<Value>,
}

impl MockDevice {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let (push, _) = broadcast::channel(64);
        let mock = Self { url, state: Arc::default(), push };
        let m = mock.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(m.clone().serve(stream));
            }
        });
        mock
    }

    async fn serve(self, stream: TcpStream) {
        let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let (sink, mut source) = ws.split();
        let sink = Arc::new(tokio::sync::Mutex::new(sink));
        let mut push = self.push.subscribe();
        loop {
            tokio::select! {
                frame = source.next() => {
                    let Some(Ok(Message::Text(text))) = frame else { return };
                    let msg: Value = serde_json::from_str(text.as_str()).unwrap();
                    let (reply, delay, notif) = self.handle(&msg);
                    let sink = sink.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                        let mut sink = sink.lock().await;
                        if let Some(r) = reply { let _ = sink.send(Message::text(r.to_string())).await; }
                        if let Some(n) = notif { let _ = sink.send(Message::text(n.to_string())).await; }
                    });
                }
                Ok(n) = push.recv() => {
                    let _ = sink.lock().await.send(Message::text(n.to_string())).await;
                }
            }
        }
    }

    fn handle(&self, msg: &Value) -> (Option<Value>, u64, Option<Value>) {
        let mut st = self.state.lock().unwrap();
        let action = msg["action"].as_str().unwrap().to_string();
        let dev = msg["deviceId"].as_str().unwrap().to_string();
        st.calls.push(action.clone());
        let delay = st.delay_ms.get(&action).copied().unwrap_or(0);
        if st.silent.contains(&action) {
            return (None, 0, None);
        }
        if st.fail.contains(&action) {
            return (Some(json!({"id": msg["id"], "status": "ERROR", "error": "simulated failure"})), delay, None);
        }
        let entry = st.devices.entry(dev.clone()).or_insert(("OFF".into(), 0));
        let mut notif = None;
        match action.as_str() {
            "powerOn" | "powerOff" => {
                entry.0 = if action == "powerOn" { "ON".into() } else { "OFF".into() };
                notif = Some(
                    json!({"type": "notification", "notification": "stateChanged", "deviceId": dev, "state": entry.0}),
                );
            }
            "setLevel" => entry.1 = msg["level"].as_u64().unwrap(),
            "reboot" => {
                entry.0 = "ON".into();
                notif = Some(json!({"type": "notification", "notification": "ready", "deviceId": dev}));
            }
            _ => {}
        }
        let data = json!({"state": entry.0, "level": entry.1});
        (Some(json!({"id": msg["id"], "status": "OK", "data": data})), delay, notif)
    }

    fn set(&self, f: impl FnOnce(&mut DeviceState)) {
        f(&mut self.state.lock().unwrap());
    }

    fn calls(&self) -> Vec<String> {
        self.state.lock().unwrap().calls.clone()
    }
}

struct Client {
    lines: tokio::io::Lines<BufReader<OwnedReadHalf>>,
    writer: OwnedWriteHalf,
}

impl Client {
    async fn connect(addr: std::net::SocketAddr) -> Self {
        let (r, w) = TcpStream::connect(addr).await.unwrap().into_split();
        Self { lines: BufReader::new(r).lines(), writer: w }
    }

    async fn send_raw(&mut self, line: &str) {
        self.writer.write_all(format!("{line}\n").as_bytes()).await.unwrap();
    }

    async fn send(&mut self, v: Value) {
        self.send_raw(&v.to_string()).await;
    }

    /// Prochain message satisfaisant `pred` (les autres sont ignorés).
    async fn recv_where(&mut self, pred: impl Fn(&Value) -> bool) -> Value {
        timeout(Duration::from_secs(5), async {
            loop {
                let line = self.lines.next_line().await.unwrap().expect("connexion fermée");
                let v: Value = serde_json::from_str(&line).unwrap();
                if pred(&v) {
                    return v;
                }
            }
        })
        .await
        .expect("message attendu non reçu")
    }

    async fn response(&mut self, id: &str) -> Value {
        self.recv_where(|v| v.get("response").is_some() && v["id"] == id).await
    }

    async fn event(&mut self, name: &str) -> Value {
        self.recv_where(|v| v["event"] == name).await
    }
}

async fn start(device: &MockDevice, extra: &[(&str, &str)]) -> Client {
    let mut vars: HashMap<String, String> = [
        ("LISTEN_ADDR", "127.0.0.1:0"),
        ("SCHEMA_DIR", concat!(env!("CARGO_MANIFEST_DIR"), "/schemas")),
        ("STEP_TIMEOUT_MS", "500"),
        ("RECONNECT_DELAY_MS", "50"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    vars.insert("WS_ENDPOINTS".into(), format!("device={}", device.url));
    for (k, v) in extra {
        vars.insert(k.to_string(), v.to_string());
    }
    let app = saisonnier::start(Settings::from_map(vars).unwrap()).await.unwrap();
    timeout(Duration::from_secs(5), app.interfaces.get("device").unwrap().wait_connected()).await.unwrap();
    Client::connect(app.local_addr).await
}

#[tokio::test]
async fn turn_on_device_done() {
    let device = MockDevice::start().await;
    let mut client = start(&device, &[]).await;
    client.send(json!({"request": "turnOnDevice", "id": "r1", "parameters": {"deviceId": "lamp-1"}})).await;
    let resp = client.response("r1").await;
    assert_eq!(resp["status"], "DONE", "{resp}");
    assert_eq!(resp["response"], "turnOnDevice");
    assert_eq!(resp["data"], json!({"deviceId": "lamp-1", "state": "ON", "alreadyInState": false}));
    assert_eq!(device.calls(), ["getState", "powerOn", "getState"]);
}

#[tokio::test]
async fn minimal_request_without_id_or_parameters() {
    let device = MockDevice::start().await;
    let mut client = start(&device, &[("DEFAULT_DEVICE_ID", "main")]).await;
    client.send(json!({"request": "turnOnDevice"})).await;
    let resp = client.recv_where(|v| v["response"] == "turnOnDevice").await;
    assert_eq!(resp["status"], "DONE", "{resp}");
    assert!(resp["id"].as_str().unwrap().starts_with("auto-"));
    assert_eq!(resp["data"]["deviceId"], "main");
}

#[tokio::test]
async fn device_refusal_gives_failed() {
    let device = MockDevice::start().await;
    device.set(|s| {
        s.fail.insert("powerOn".into());
    });
    let mut client = start(&device, &[]).await;
    client.send(json!({"request": "turnOnDevice", "id": "r1"})).await;
    let resp = client.response("r1").await;
    assert_eq!(resp["status"], "FAILED");
    assert_eq!(resp["error"]["code"], "STEP_FAILED");
    assert_eq!(resp["error"]["step"], "power");
}

#[tokio::test]
async fn step_timeout_gives_failed() {
    let device = MockDevice::start().await;
    device.set(|s| {
        s.silent.insert("getState".into());
    });
    let mut client = start(&device, &[("STEP_TIMEOUT_MS", "200")]).await;
    client.send(json!({"request": "getDeviceStatus", "id": "r1"})).await;
    let resp = client.response("r1").await;
    assert_eq!(resp["status"], "FAILED");
    assert_eq!(resp["error"]["code"], "TIMEOUT");
    assert_eq!(resp["error"]["step"], "read_state");
}

#[tokio::test]
async fn global_timeout_per_request() {
    let device = MockDevice::start().await;
    device.set(|s| {
        s.delay_ms.insert("getState".into(), 150);
    });
    let mut client = start(&device, &[("TIMEOUT_TURN_ON_DEVICE_MS", "250")]).await;
    client.send(json!({"request": "turnOnDevice", "id": "r1"})).await;
    let resp = client.response("r1").await;
    assert_eq!(resp["error"]["code"], "TIMEOUT", "{resp}");
    // 150 ms (read_state) + 150 ms (verify_state) > 250 ms
    assert_eq!(resp["error"]["step"], "verify_state");
    // getDeviceStatus garde le timeout par défaut.
    client.send(json!({"request": "getDeviceStatus", "id": "r2"})).await;
    assert_eq!(client.response("r2").await["status"], "DONE");
}

#[tokio::test]
async fn duplicates_are_rejected_and_fifo_is_sequential() {
    let device = MockDevice::start().await;
    device.set(|s| {
        s.delay_ms.insert("powerOn".into(), 300);
    });
    let mut client = start(&device, &[]).await;
    client.send(json!({"request": "turnOnDevice", "id": "a", "parameters": {"deviceId": "x"}})).await;
    client.send(json!({"request": "turnOnDevice", "id": "a", "parameters": {"deviceId": "y"}})).await; // même id
    client.send(json!({"request": "turnOnDevice", "id": "b", "parameters": {"deviceId": "x"}})).await; // même contenu
    client.send(json!({"request": "turnOffDevice", "id": "c", "parameters": {"deviceId": "x"}})).await;

    let first = client.recv_where(|v| v.get("response").is_some()).await;
    assert_eq!((first["id"].as_str(), first["error"]["code"].as_str()), (Some("a"), Some("DUPLICATE")));
    let second = client.recv_where(|v| v.get("response").is_some()).await;
    assert_eq!((second["id"].as_str(), second["error"]["code"].as_str()), (Some("b"), Some("DUPLICATE")));
    let third = client.recv_where(|v| v.get("response").is_some()).await;
    assert_eq!((third["id"].as_str(), third["status"].as_str()), (Some("a"), Some("DONE")));
    let fourth = client.recv_where(|v| v.get("response").is_some()).await;
    assert_eq!((fourth["id"].as_str(), fourth["status"].as_str()), (Some("c"), Some("DONE")));
    assert_eq!(fourth["data"]["state"], "OFF");

    // Une fois traitée, la même requête est de nouveau acceptée.
    client.send(json!({"request": "turnOnDevice", "id": "a", "parameters": {"deviceId": "x"}})).await;
    assert_eq!(client.response("a").await["status"], "DONE");
}

#[tokio::test]
async fn invalid_requests_are_rejected() {
    let device = MockDevice::start().await;
    let mut client = start(&device, &[]).await;
    client.send_raw("{not json").await;
    let resp = client.recv_where(|v| v["response"] == "unknown").await;
    assert_eq!(resp["error"]["code"], "INVALID_REQUEST");

    client.send(json!({"request": "setDeviceLevel", "id": "r1", "parameters": {"level": 500}})).await;
    let resp = client.response("r1").await;
    assert_eq!(resp["error"]["code"], "INVALID_REQUEST");
    assert_eq!(resp["response"], "setDeviceLevel");

    client.send(json!({"request": "setDeviceLevel", "id": "r2", "parameters": {"level": 42}})).await;
    let resp = client.response("r2").await;
    assert_eq!(resp["status"], "DONE", "{resp}");
    assert_eq!(resp["data"]["level"], 42);
}

#[tokio::test]
async fn steps_toggled_by_environment() {
    let device = MockDevice::start().await;
    let mut client = start(
        &device,
        &[
            ("DISABLED_STEPS", "turnOnDevice.read_state,turnOnDevice.verify_state"),
            ("ENABLED_STEPS", "turnOnDevice.notify"),
            ("PAUSE_TURN_ON_DEVICE_SETTLE_MS", "50"),
            ("DISABLED_TRIGGERS", "onStateChanged"),
        ],
    )
    .await;
    client.send(json!({"request": "turnOnDevice", "id": "r1", "parameters": {"deviceId": "lamp"}})).await;
    let event = client.event("deviceTurnedOn").await;
    assert_eq!(event["source"], "r1");
    assert_eq!(client.response("r1").await["status"], "DONE");
    assert_eq!(device.calls(), ["powerOn"]);
}

#[tokio::test]
async fn spontaneous_notifications_produce_events() {
    let device = MockDevice::start().await;
    let mut client = start(&device, &[]).await;
    device
        .push
        .send(json!({"type": "notification", "notification": "alarm", "deviceId": "lamp", "alarm": "overheat"}))
        .unwrap();
    let event = client.event("deviceAlarm").await;
    assert_eq!(event["data"]["alarm"], "overheat");
    assert_eq!(event["source"], "onAlarm");

    // Changement d'état provoqué par une requête : réponse puis évènement.
    client.send(json!({"request": "turnOffDevice", "id": "r1", "parameters": {"deviceId": "lamp"}})).await;
    client.send(json!({"request": "turnOnDevice", "id": "r2", "parameters": {"deviceId": "lamp"}})).await;
    let resp = client.response("r2").await;
    assert_eq!(resp["status"], "DONE");
    let event = client.event("deviceStateChanged").await;
    assert_eq!(event["data"], json!({"deviceId": "lamp", "state": "ON", "level": 0}));
}

#[tokio::test]
async fn reboot_waits_for_ready_notification() {
    let device = MockDevice::start().await;
    device.set(|s| {
        s.delay_ms.insert("reboot".into(), 100);
    });
    let mut client = start(&device, &[]).await;
    client.send(json!({"request": "rebootDevice", "id": "r1", "parameters": {"deviceId": "lamp"}})).await;
    let resp = client.response("r1").await;
    assert_eq!(resp["status"], "DONE", "{resp}");
    assert_eq!(device.calls(), ["reboot", "getState"]);
}

#[tokio::test]
async fn unreachable_interface_fails_fast() {
    let device = MockDevice::start().await;
    let mut client = start(&device, &[("ENABLED_STEPS", "rebootDevice.maintenance_ack")]).await;
    client.send(json!({"request": "rebootDevice", "id": "r1"})).await;
    let resp = client.response("r1").await;
    assert_eq!(resp["error"]["code"], "INTERFACE_UNAVAILABLE", "{resp}");
    assert_eq!(resp["error"]["step"], "maintenance_ack");
}

#[tokio::test]
async fn responses_and_events_routed_to_tcp_servers() {
    let device = MockDevice::start().await;
    let server_1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let servers = format!("server_1={}", server_1.local_addr().unwrap());
    let mut client = start(
        &device,
        &[("TCP_SERVERS", &servers), ("RESPONSE_TARGETS", "origin,server_1"), ("EVENT_TARGETS", "server_1")],
    )
    .await;
    let (sock, _) = timeout(Duration::from_secs(5), server_1.accept()).await.unwrap().unwrap();
    let mut lines = BufReader::new(sock).lines();

    client.send(json!({"request": "turnOnDevice", "id": "r1"})).await;
    assert_eq!(client.response("r1").await["status"], "DONE");
    let mut received = Vec::new();
    for _ in 0..2 {
        let line = timeout(Duration::from_secs(5), lines.next_line()).await.unwrap().unwrap().unwrap();
        received.push(serde_json::from_str::<Value>(&line).unwrap());
    }
    assert_eq!(received[0]["response"], "turnOnDevice");
    assert_eq!(received[1]["event"], "deviceStateChanged");
}
