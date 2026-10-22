//! Validation JSON Schema des messages entrants et sortants.
//!
//! Les schémas sont lus au démarrage depuis `SCHEMA_DIR`. Les `$ref` vers
//! d'autres fichiers (`common.schema.json#/...`) sont résolus dans ce même
//! répertoire, quel que soit l'hôte de leur `$id`.

use std::path::{Path, PathBuf};

use anyhow::Context;
use jsonschema::{Retrieve, Uri, Validator};
use serde_json::Value;

pub const REQUEST_SCHEMA: &str = "request.schema.json";
pub const RESPONSE_SCHEMA: &str = "response.schema.json";
pub const EVENT_SCHEMA: &str = "event.schema.json";

pub struct Schemas {
    request: Validator,
    response: Validator,
    event: Validator,
}

impl Schemas {
    pub fn load(dir: &Path) -> anyhow::Result<Self> {
        Ok(Self {
            request: compile(dir, REQUEST_SCHEMA)?,
            response: compile(dir, RESPONSE_SCHEMA)?,
            event: compile(dir, EVENT_SCHEMA)?,
        })
    }

    pub fn validate_request(&self, v: &Value) -> Result<(), String> {
        check(&self.request, v)
    }

    pub fn validate_response(&self, v: &Value) -> Result<(), String> {
        check(&self.response, v)
    }

    pub fn validate_event(&self, v: &Value) -> Result<(), String> {
        check(&self.event, v)
    }
}

fn compile(dir: &Path, file: &str) -> anyhow::Result<Validator> {
    let path = dir.join(file);
    let schema = read_json(&path).map_err(|e| anyhow::anyhow!(e.to_string()))?;
    jsonschema::options()
        .with_retriever(DirRetriever(dir.to_path_buf()))
        .build(&schema)
        .map_err(|e| anyhow::anyhow!("{e}"))
        .with_context(|| format!("schéma invalide : {}", path.display()))
}

fn check(validator: &Validator, v: &Value) -> Result<(), String> {
    let errors: Vec<String> = validator
        .iter_errors(v)
        .map(|e| {
            let at = e.instance_path().to_string();
            if at.is_empty() { e.to_string() } else { format!("{at}: {e}") }
        })
        .collect();
    if errors.is_empty() { Ok(()) } else { Err(errors.join("; ")) }
}

fn read_json(path: &Path) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("lecture de {} impossible : {e}", path.display()))?;
    Ok(serde_json::from_str(&text).map_err(|e| format!("JSON invalide dans {} : {e}", path.display()))?)
}

/// Résout `.../<fichier>.json` vers `SCHEMA_DIR/<fichier>.json`.
struct DirRetriever(PathBuf);

impl Retrieve for DirRetriever {
    fn retrieve(&self, uri: &Uri<String>) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let file = uri.as_str().split('#').next().unwrap_or("").rsplit('/').next().unwrap_or("");
        if file.is_empty() || file.contains("..") {
            return Err(format!("référence de schéma non résolue : {}", uri.as_str()).into());
        }
        read_json(&self.0.join(file))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schemas() -> Schemas {
        Schemas::load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("schemas")).unwrap()
    }

    #[test]
    fn request_validation() {
        let s = schemas();
        assert!(s.validate_request(&json!({"request": "turnOnDevice"})).is_ok());
        assert!(
            s.validate_request(&json!({"request": "turnOnDevice", "id": "1", "parameters": {"deviceId": "lamp-1"}}))
                .is_ok()
        );
        assert!(s.validate_request(&json!({"request": "unknown"})).is_err());
        assert!(s.validate_request(&json!({"request": "setDeviceLevel"})).is_err());
        assert!(s.validate_request(&json!({"request": "setDeviceLevel", "parameters": {"level": 101}})).is_err());
        assert!(s.validate_request(&json!({"request": "turnOnDevice", "parameters": {"foo": 1}})).is_err());
    }

    #[test]
    fn response_requires_error_when_failed() {
        let s = schemas();
        let ok = json!({"response": "turnOnDevice", "id": "1", "status": "DONE", "data": {}, "timestamp": 1});
        assert!(s.validate_response(&ok).is_ok());
        let bad = json!({"response": "turnOnDevice", "id": "1", "status": "FAILED", "timestamp": 1});
        assert!(s.validate_response(&bad).is_err());
    }
}
