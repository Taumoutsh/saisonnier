//! Modèles d'échange JSON.
//!
//! - Entrée : une requête JSON (validée par `schemas/request.schema.json`) est
//!   décomposée en [`InboundRequest`] contenant une [`Command`] typée.
//! - Sortie : [`Response`] et [`Event`] sont sérialisés en JSON (validés par
//!   `schemas/response.schema.json` et `schemas/event.schema.json`).

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Requête reçue, décomposée dans le modèle interne.
#[derive(Debug, Clone)]
pub struct InboundRequest {
    /// Identifiant fourni par le client, ou généré (`auto-<n>`) s'il est absent.
    pub id: String,
    pub command: Command,
    /// Paramètres bruts, conservés pour la détection de doublons.
    pub raw_parameters: Value,
}

impl InboundRequest {
    pub fn name(&self) -> &'static str {
        self.command.name()
    }
}

/// Une variante par type de requête (`request`), avec ses paramètres propres.
///
/// Ajouter une requête : ajouter une variante ici, l'ajouter dans
/// `schemas/` et lui associer un workflow dans `workflows::plan`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "request", content = "parameters", rename_all = "camelCase")]
pub enum Command {
    TurnOnDevice(DeviceParams),
    TurnOffDevice(DeviceParams),
    GetDeviceStatus(DeviceParams),
    SetDeviceLevel(LevelParams),
    RebootDevice(DeviceParams),
}

impl Command {
    pub fn name(&self) -> &'static str {
        match self {
            Command::TurnOnDevice(_) => "turnOnDevice",
            Command::TurnOffDevice(_) => "turnOffDevice",
            Command::GetDeviceStatus(_) => "getDeviceStatus",
            Command::SetDeviceLevel(_) => "setDeviceLevel",
            Command::RebootDevice(_) => "rebootDevice",
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceParams {
    pub device_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LevelParams {
    pub device_id: Option<String>,
    pub level: u8,
}

/// Enveloppe brute d'une requête, avant typage.
#[derive(Debug, Deserialize)]
struct RawRequest {
    request: String,
    id: Option<String>,
    #[serde(default)]
    parameters: Option<Value>,
}

/// Décompose un JSON (déjà validé par schéma) en requête interne.
pub fn parse_request(value: &Value, auto_id: impl FnOnce() -> String) -> Result<InboundRequest, String> {
    let raw: RawRequest = serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
    let parameters = raw.parameters.unwrap_or_else(|| json!({}));
    let command: Command = serde_json::from_value(json!({ "request": raw.request, "parameters": parameters }))
        .map_err(|e| e.to_string())?;
    Ok(InboundRequest { id: raw.id.unwrap_or_else(auto_id), command, raw_parameters: parameters })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Status {
    Done,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    InvalidRequest,
    Duplicate,
    QueueFull,
    Timeout,
    StepFailed,
    InterfaceUnavailable,
    Internal,
}

#[derive(Debug, Clone, Serialize)]
pub struct ErrorInfo {
    pub code: ErrorCode,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Response {
    pub response: String,
    pub id: String,
    pub status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorInfo>,
    pub timestamp: u64,
}

impl Response {
    pub fn done(name: &str, id: &str, data: Value) -> Self {
        Self {
            response: name.to_string(),
            id: id.to_string(),
            status: Status::Done,
            data: Some(data),
            error: None,
            timestamp: now_ms(),
        }
    }

    pub fn failed(name: &str, id: &str, error: ErrorInfo) -> Self {
        Self {
            response: name.to_string(),
            id: id.to_string(),
            status: Status::Failed,
            data: None,
            error: Some(error),
            timestamp: now_ms(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Event {
    pub event: String,
    /// Requête ou déclencheur à l'origine de l'évènement.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    pub data: Value,
    pub timestamp: u64,
}

impl Event {
    pub fn new(event: &str, source: Option<String>, data: Value) -> Self {
        Self { event: event.to_string(), source, data, timestamp: now_ms() }
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_request_without_parameters() {
        let req = parse_request(&json!({"request": "turnOnDevice"}), || "auto-1".into()).unwrap();
        assert_eq!(req.id, "auto-1");
        assert!(matches!(req.command, Command::TurnOnDevice(DeviceParams { device_id: None })));
    }

    #[test]
    fn parses_typed_parameters() {
        let req = parse_request(
            &json!({"request": "setDeviceLevel", "id": "r1", "parameters": {"deviceId": "lamp", "level": 42}}),
            || unreachable!(),
        )
        .unwrap();
        match req.command {
            Command::SetDeviceLevel(p) => {
                assert_eq!(p.level, 42);
                assert_eq!(p.device_id.as_deref(), Some("lamp"));
            }
            other => panic!("{other:?}"),
        }
    }
}
