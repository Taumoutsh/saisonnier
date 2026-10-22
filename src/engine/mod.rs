//! Moteur : FIFO de traitement (une seule requête à la fois), rejet des
//! doublons, exécution des workflows et déclencheurs sur messages spontanés.

pub mod executor;
pub mod workflow;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use serde_json::Value;
use thiserror::Error;
use tokio::sync::{broadcast::error::RecvError, mpsc};
use tracing::{error, info, warn};

use crate::config::{DedupMode, Settings};
use crate::interfaces::Interfaces;
use crate::model::{ErrorCode, ErrorInfo, InboundRequest, Response};
use crate::router::Router;
use crate::server::ClientId;
use workflow::{Ctx, Workflow};

/// Dépendances partagées par les workflows en cours d'exécution.
pub struct Runtime {
    pub settings: Arc<Settings>,
    pub interfaces: Interfaces,
    pub router: Arc<Router>,
}

/// Construit le workflow d'un déclencheur à partir du message reçu.
pub type TriggerPlanner = Box<dyn Fn(&Value, &Settings) -> Workflow + Send + Sync>;

/// Associe une requête typée à son workflow.
pub type Planner = fn(&InboundRequest, &Settings) -> Workflow;

/// Message spontané reçu sur une interface et déclenchant un workflow
/// (typiquement terminé par l'émission d'un évènement).
pub struct Trigger {
    pub name: String,
    pub interface: String,
    pub matches: Box<dyn Fn(&Value) -> bool + Send + Sync>,
    pub plan: TriggerPlanner,
}

#[derive(Debug, Error)]
pub enum Rejection {
    #[error("requête en doublon d'une requête en attente ou en cours ({0})")]
    Duplicate(String),
    #[error("file d'attente pleine")]
    QueueFull,
}

impl From<Rejection> for ErrorInfo {
    fn from(r: Rejection) -> Self {
        let code = match r {
            Rejection::Duplicate(_) => ErrorCode::Duplicate,
            Rejection::QueueFull => ErrorCode::QueueFull,
        };
        ErrorInfo { code, message: r.to_string(), step: None }
    }
}

enum Job {
    Request { req: InboundRequest, origin: Option<ClientId>, keys: Vec<String> },
    Trigger { trigger: Arc<Trigger>, message: Value },
}

#[derive(Clone)]
pub struct Engine {
    tx: mpsc::Sender<Job>,
    pending: Arc<Mutex<HashSet<String>>>,
    dedup: DedupMode,
}

impl Engine {
    pub fn start(rt: Arc<Runtime>, planner: Planner) -> Self {
        let (tx, rx) = mpsc::channel(rt.settings.queue_capacity.max(1));
        let pending = Arc::new(Mutex::new(HashSet::new()));
        tokio::spawn(worker(rx, rt.clone(), pending.clone(), planner));
        Self { tx, pending, dedup: rt.settings.dedup }
    }

    /// Met une requête en file, ou la rejette (doublon, file pleine).
    pub fn submit(&self, req: InboundRequest, origin: Option<ClientId>) -> Result<(), Rejection> {
        let keys = self.dedup_keys(&req);
        let mut pending = self.pending.lock().expect("verrou non empoisonné");
        if let Some(key) = keys.iter().find(|k| pending.contains(*k)) {
            return Err(Rejection::Duplicate(key.clone()));
        }
        let (id, name) = (req.id.clone(), req.name());
        self.tx.try_send(Job::Request { req, origin, keys: keys.clone() }).map_err(|_| Rejection::QueueFull)?;
        pending.extend(keys);
        info!(%id, request = name, queued = self.tx.max_capacity() - self.tx.capacity(), "requête en file");
        Ok(())
    }

    fn trigger(&self, trigger: Arc<Trigger>, message: Value) -> Result<(), Rejection> {
        self.tx.try_send(Job::Trigger { trigger, message }).map_err(|_| Rejection::QueueFull)
    }

    /// Écoute les interfaces et met en file les déclencheurs correspondants.
    pub fn listen_triggers(&self, interfaces: &Interfaces, triggers: Vec<Trigger>) {
        let triggers: Vec<Arc<Trigger>> = triggers.into_iter().map(Arc::new).collect();
        for iface in interfaces.iter() {
            let mine: Vec<Arc<Trigger>> = triggers.iter().filter(|t| t.interface == iface.name()).cloned().collect();
            if mine.is_empty() {
                continue;
            }
            let mut rx = iface.subscribe();
            let engine = self.clone();
            let name = iface.name().to_string();
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(msg) => {
                            for t in mine.iter().filter(|t| (t.matches)(&msg)) {
                                info!(trigger = %t.name, interface = %name, "déclencheur");
                                if let Err(e) = engine.trigger(t.clone(), msg.clone()) {
                                    warn!(trigger = %t.name, error = %e, "déclencheur ignoré");
                                }
                            }
                        }
                        Err(RecvError::Lagged(n)) => warn!(interface = %name, skipped = n, "notifications perdues"),
                        Err(RecvError::Closed) => return,
                    }
                }
            });
        }
    }

    fn dedup_keys(&self, req: &InboundRequest) -> Vec<String> {
        let mut keys = Vec::new();
        if matches!(self.dedup, DedupMode::Id | DedupMode::Both) {
            keys.push(format!("id:{}", req.id));
        }
        if matches!(self.dedup, DedupMode::Content | DedupMode::Both) {
            // serde_json trie les clés des objets : représentation canonique.
            keys.push(format!("content:{}:{}", req.name(), req.raw_parameters));
        }
        keys
    }
}

async fn worker(mut rx: mpsc::Receiver<Job>, rt: Arc<Runtime>, pending: Arc<Mutex<HashSet<String>>>, planner: Planner) {
    while let Some(job) = rx.recv().await {
        match job {
            Job::Request { req, origin, keys } => {
                let (id, name) = (req.id.clone(), req.name());
                info!(%id, request = name, "début du traitement");
                let task_rt = rt.clone();
                // Tâche dédiée : une panique dans un workflow ne tue pas la FIFO.
                let outcome = tokio::spawn(async move {
                    let wf = planner(&req, &task_rt.settings);
                    let mut ctx = Ctx::new(&req.id, &wf.name);
                    executor::run(&wf, &mut ctx, &task_rt).await
                })
                .await;
                let response = match outcome {
                    Ok(Ok(data)) => Response::done(name, &id, data),
                    Ok(Err(failure)) => Response::failed(name, &id, failure.into()),
                    Err(e) => Response::failed(
                        name,
                        &id,
                        ErrorInfo { code: ErrorCode::Internal, message: format!("erreur interne : {e}"), step: None },
                    ),
                };
                match &response.error {
                    None => info!(%id, request = name, "DONE"),
                    Some(err) => {
                        warn!(%id, request = name, code = ?err.code, step = ?err.step, message = %err.message, "FAILED")
                    }
                }
                {
                    let mut pending = pending.lock().expect("verrou non empoisonné");
                    for k in &keys {
                        pending.remove(k);
                    }
                }
                rt.router.send_response(&response, origin);
            }
            Job::Trigger { trigger, message } => {
                let task_rt = rt.clone();
                let name = trigger.name.clone();
                let outcome = tokio::spawn(async move {
                    let wf = (trigger.plan)(&message, &task_rt.settings);
                    let mut ctx = Ctx::new(&trigger.name, &wf.name).with_var("trigger", message);
                    executor::run(&wf, &mut ctx, &task_rt).await
                })
                .await;
                match outcome {
                    Ok(Ok(_)) => info!(trigger = %name, "séquence terminée"),
                    Ok(Err(f)) => {
                        warn!(trigger = %name, code = ?f.code, step = ?f.step, message = %f.message, "séquence en échec")
                    }
                    Err(e) => error!(trigger = %name, error = %e, "séquence interrompue"),
                }
            }
        }
    }
}
