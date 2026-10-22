//! Interfaces externes (WebSocket, serveurs TCP sortants, ...).
//!
//! Une [`Interface`] est une poignée indépendante du transport : on y pousse
//! des messages JSON et on s'abonne aux messages JSON reçus. Chaque transport
//! est un « driver » : une tâche qui reçoit un [`DriverIo`] et fait le pont
//! entre ces canaux et le réseau. Ajouter un nouveau type d'interface (MQTT,
//! HTTP, série, ...) revient à écrire un nouveau driver ; le moteur et les
//! workflows n'en savent rien, ils désignent les interfaces par leur nom.

pub mod tcp;
pub mod websocket;

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, watch};

const OUTGOING_CAPACITY: usize = 1024;
const INCOMING_CAPACITY: usize = 1024;

#[derive(Debug, Error)]
pub enum InterfaceError {
    #[error("interface « {0} » non connectée")]
    NotConnected(String),
    #[error("file d'envoi de l'interface « {0} » pleine")]
    Busy(String),
    #[error("interface « {0} » arrêtée")]
    Closed(String),
}

#[derive(Clone)]
pub struct Interface {
    inner: Arc<Inner>,
}

struct Inner {
    name: String,
    kind: &'static str,
    outgoing: mpsc::Sender<Value>,
    incoming: broadcast::Sender<Value>,
    connected: watch::Receiver<bool>,
}

/// Extrémités des canaux remises au driver de transport.
pub struct DriverIo {
    /// Messages à émettre sur le réseau.
    pub outgoing: mpsc::Receiver<Value>,
    /// Messages reçus du réseau, diffusés à tous les abonnés.
    pub incoming: broadcast::Sender<Value>,
    /// État de connexion publié par le driver.
    pub connected: watch::Sender<bool>,
}

/// Crée une interface et les canaux de son driver.
pub fn channel(name: &str, kind: &'static str) -> (Interface, DriverIo) {
    let (out_tx, out_rx) = mpsc::channel(OUTGOING_CAPACITY);
    let (in_tx, _) = broadcast::channel(INCOMING_CAPACITY);
    let (conn_tx, conn_rx) = watch::channel(false);
    let iface = Interface {
        inner: Arc::new(Inner {
            name: name.to_string(),
            kind,
            outgoing: out_tx,
            incoming: in_tx.clone(),
            connected: conn_rx,
        }),
    };
    (iface, DriverIo { outgoing: out_rx, incoming: in_tx, connected: conn_tx })
}

impl Interface {
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn kind(&self) -> &'static str {
        self.inner.kind
    }

    pub fn is_connected(&self) -> bool {
        *self.inner.connected.borrow()
    }

    /// Attend que l'interface soit connectée.
    pub async fn wait_connected(&self) {
        let mut rx = self.inner.connected.clone();
        let _ = rx.wait_for(|c| *c).await;
    }

    /// Envoi immédiat : échoue si l'interface n'est pas connectée (utilisé
    /// par les actions, pour échouer vite plutôt que d'envoyer en différé).
    pub fn send(&self, msg: Value) -> Result<(), InterfaceError> {
        if !self.is_connected() {
            return Err(InterfaceError::NotConnected(self.inner.name.clone()));
        }
        self.enqueue(msg)
    }

    /// Envoi différé : le message est mis en file et partira à la
    /// (re)connexion (utilisé pour les réponses / évènements sortants).
    pub fn enqueue(&self, msg: Value) -> Result<(), InterfaceError> {
        self.inner.outgoing.try_send(msg).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => InterfaceError::Busy(self.inner.name.clone()),
            mpsc::error::TrySendError::Closed(_) => InterfaceError::Closed(self.inner.name.clone()),
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.inner.incoming.subscribe()
    }
}

/// Registre des interfaces, indexé par nom.
#[derive(Clone, Default)]
pub struct Interfaces {
    map: HashMap<String, Interface>,
}

impl Interfaces {
    pub fn insert(&mut self, iface: Interface) {
        self.map.insert(iface.name().to_string(), iface);
    }

    pub fn get(&self, name: &str) -> Option<&Interface> {
        self.map.get(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Interface> {
        self.map.values()
    }
}
