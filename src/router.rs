//! Routage des réponses et évènements vers leurs destinations
//! (`RESPONSE_TARGETS`, `EVENT_TARGETS`), après validation par schéma.

use std::sync::Arc;

use serde_json::Value;
use tracing::{error, warn};

use crate::config::{Settings, Target};
use crate::interfaces::Interfaces;
use crate::model::{Event, Response};
use crate::schema::Schemas;
use crate::server::{ClientId, Clients};

pub struct Router {
    clients: Arc<Clients>,
    interfaces: Interfaces,
    schemas: Arc<Schemas>,
    response_targets: Vec<Target>,
    event_targets: Vec<Target>,
    validate: bool,
}

impl Router {
    pub fn new(settings: &Settings, clients: Arc<Clients>, interfaces: Interfaces, schemas: Arc<Schemas>) -> Self {
        Self {
            clients,
            interfaces,
            schemas,
            response_targets: settings.response_targets.clone(),
            event_targets: settings.event_targets.clone(),
            validate: settings.validate_output,
        }
    }

    pub fn send_response(&self, response: &Response, origin: Option<ClientId>) {
        let value = serde_json::to_value(response).expect("Response est sérialisable");
        if self.validate
            && let Err(e) = self.schemas.validate_response(&value)
        {
            error!(%value, error = %e, "réponse non conforme au schéma (envoyée quand même)");
        }
        self.dispatch(&value, &self.response_targets, origin);
    }

    pub fn send_event(&self, event: &Event) {
        let value = serde_json::to_value(event).expect("Event est sérialisable");
        if self.validate
            && let Err(e) = self.schemas.validate_event(&value)
        {
            error!(%value, error = %e, "évènement non conforme au schéma (envoyé quand même)");
        }
        self.dispatch(&value, &self.event_targets, None);
    }

    fn dispatch(&self, value: &Value, targets: &[Target], origin: Option<ClientId>) {
        for target in targets {
            match target {
                Target::Origin => {
                    if let Some(id) = origin
                        && !self.clients.send(id, value.clone())
                    {
                        warn!(client = id, "client d'origine déconnecté, réponse perdue");
                    }
                }
                Target::Clients => self.clients.broadcast(value),
                Target::Server(name) => match self.interfaces.get(name) {
                    Some(iface) => {
                        if let Err(e) = iface.enqueue(value.clone()) {
                            warn!(server = %name, error = %e, "message sortant perdu");
                        }
                    }
                    None => warn!(server = %name, "destination inconnue (absente de TCP_SERVERS)"),
                },
            }
        }
    }
}
