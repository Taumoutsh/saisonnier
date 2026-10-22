//! Serveur TCP d'entrée : reçoit des requêtes JSON (une par ligne), les
//! valide, les décompose et les soumet au moteur.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{debug, info, warn};

use crate::engine::Engine;
use crate::interfaces::tcp::MAX_LINE_LENGTH;
use crate::model::{ErrorCode, ErrorInfo, Response, parse_request};
use crate::router::Router;
use crate::schema::Schemas;

pub type ClientId = u64;

const CLIENT_QUEUE: usize = 256;

/// Registre des clients TCP connectés.
#[derive(Default)]
pub struct Clients {
    map: Mutex<HashMap<ClientId, mpsc::Sender<Value>>>,
}

impl Clients {
    fn register(&self, id: ClientId, tx: mpsc::Sender<Value>) {
        self.map.lock().expect("verrou").insert(id, tx);
    }

    fn unregister(&self, id: ClientId) {
        self.map.lock().expect("verrou").remove(&id);
    }

    /// Renvoie `false` si le client n'est plus connecté.
    pub fn send(&self, id: ClientId, msg: Value) -> bool {
        match self.map.lock().expect("verrou").get(&id) {
            Some(tx) => tx.try_send(msg).is_ok(),
            None => false,
        }
    }

    pub fn broadcast(&self, msg: &Value) {
        for tx in self.map.lock().expect("verrou").values() {
            let _ = tx.try_send(msg.clone());
        }
    }

    pub fn count(&self) -> usize {
        self.map.lock().expect("verrou").len()
    }
}

/// Contexte partagé par les connexions entrantes.
pub struct Inbound {
    pub clients: Arc<Clients>,
    pub schemas: Arc<Schemas>,
    pub engine: Engine,
    pub router: Arc<Router>,
}

static NEXT_CLIENT: AtomicU64 = AtomicU64::new(1);
static NEXT_AUTO_ID: AtomicU64 = AtomicU64::new(1);

pub async fn serve(listener: TcpListener, inbound: Arc<Inbound>) {
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let id = NEXT_CLIENT.fetch_add(1, Ordering::Relaxed);
                info!(client = id, %peer, "client connecté");
                tokio::spawn(handle_client(id, stream, inbound.clone()));
            }
            Err(e) => warn!(error = %e, "accept() en échec"),
        }
    }
}

async fn handle_client(id: ClientId, stream: TcpStream, inbound: Arc<Inbound>) {
    let (mut sink, mut lines) = Framed::new(stream, LinesCodec::new_with_max_length(MAX_LINE_LENGTH)).split();
    let (tx, mut rx) = mpsc::channel::<Value>(CLIENT_QUEUE);
    inbound.clients.register(id, tx);

    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            debug!(client = id, %msg, "-> client");
            if sink.send(msg.to_string()).await.is_err() {
                break;
            }
        }
    });

    while let Some(line) = lines.next().await {
        match line {
            Ok(line) if line.trim().is_empty() => {}
            Ok(line) => handle_line(id, &line, &inbound),
            Err(e) => {
                warn!(client = id, error = %e, "lecture impossible, déconnexion");
                break;
            }
        }
    }

    inbound.clients.unregister(id);
    writer.abort();
    info!(client = id, "client déconnecté");
}

fn handle_line(client: ClientId, line: &str, inbound: &Inbound) {
    debug!(client, %line, "<- client");
    let reject = |name: &str, id: &str, error: ErrorInfo| {
        warn!(client, request = name, %id, code = ?error.code, message = %error.message, "requête rejetée");
        inbound.router.send_response(&Response::failed(name, id, error), Some(client));
    };
    let invalid = |message: String| ErrorInfo { code: ErrorCode::InvalidRequest, message, step: None };

    let value: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => return reject("unknown", "unknown", invalid(format!("JSON invalide : {e}"))),
    };
    // Nom et id best-effort, pour pouvoir répondre même à une requête invalide.
    let name = value.get("request").and_then(Value::as_str).filter(|s| !s.is_empty()).unwrap_or("unknown");
    let id = value.get("id").and_then(Value::as_str).filter(|s| !s.is_empty()).unwrap_or("unknown");

    if let Err(e) = inbound.schemas.validate_request(&value) {
        return reject(name, id, invalid(format!("non conforme au schéma : {e}")));
    }
    let request = match parse_request(&value, || format!("auto-{}", NEXT_AUTO_ID.fetch_add(1, Ordering::Relaxed))) {
        Ok(r) => r,
        Err(e) => return reject(name, id, invalid(e)),
    };
    let (name, id) = (request.name(), request.id.clone());
    if let Err(rejection) = inbound.engine.submit(request, Some(client)) {
        reject(name, &id, rejection.into());
    }
}
