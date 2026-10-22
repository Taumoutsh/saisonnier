//! Configuration chargée depuis les variables d'environnement.
//!
//! Toutes les clés sont lues une seule fois au démarrage et conservées dans
//! [`Settings`]. Les clés « dynamiques » (timeouts par requête, pauses par
//! étape, ...) sont résolues à la demande à partir de la même photographie de
//! l'environnement, ce qui permet aussi de construire une configuration de
//! test à partir d'une simple `HashMap`.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, bail};

/// Destination d'un message sortant (réponse ou évènement).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// Le client TCP qui a émis la requête (réponses uniquement).
    Origin,
    /// Tous les clients TCP connectés.
    Clients,
    /// Un serveur TCP sortant, désigné par son nom (`TCP_SERVERS`).
    Server(String),
}

/// Politique de détection des doublons dans la FIFO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupMode {
    Off,
    /// Même `id` qu'une requête en attente ou en cours.
    Id,
    /// Même `request` + mêmes `parameters` qu'une requête en attente ou en cours.
    Content,
    Both,
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub listen_addr: String,
    /// Interfaces WebSocket : `nom -> url`.
    pub ws_endpoints: Vec<(String, String)>,
    /// Serveurs TCP sortants : `nom -> host:port`.
    pub tcp_servers: Vec<(String, String)>,
    pub response_targets: Vec<Target>,
    pub event_targets: Vec<Target>,
    pub schema_dir: PathBuf,
    pub validate_output: bool,
    pub default_timeout: Duration,
    pub step_timeout: Duration,
    pub queue_capacity: usize,
    pub dedup: DedupMode,
    pub reconnect_delay: Duration,
    pub default_device_id: String,
    disabled_steps: HashSet<String>,
    enabled_steps: HashSet<String>,
    disabled_triggers: HashSet<String>,
    vars: HashMap<String, String>,
}

impl Settings {
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_map(std::env::vars().collect())
    }

    pub fn from_map(vars: HashMap<String, String>) -> anyhow::Result<Self> {
        let get = |k: &str| vars.get(k).map(|v| v.trim()).filter(|v| !v.is_empty());
        let ms = |k: &str, default: u64| -> anyhow::Result<Duration> {
            Ok(Duration::from_millis(match get(k) {
                Some(v) => v.parse().with_context(|| format!("{k} doit être un entier (ms)"))?,
                None => default,
            }))
        };

        let dedup = match get("DEDUP_MODE").unwrap_or("both").to_ascii_lowercase().as_str() {
            "off" | "none" => DedupMode::Off,
            "id" => DedupMode::Id,
            "content" => DedupMode::Content,
            "both" => DedupMode::Both,
            other => bail!("DEDUP_MODE invalide : {other} (off|id|content|both)"),
        };

        Ok(Self {
            listen_addr: get("LISTEN_ADDR").unwrap_or("0.0.0.0:7000").to_string(),
            ws_endpoints: parse_named(get("WS_ENDPOINTS").unwrap_or("device=ws://127.0.0.1:9001"))
                .context("WS_ENDPOINTS")?,
            tcp_servers: parse_named(get("TCP_SERVERS").unwrap_or("")).context("TCP_SERVERS")?,
            response_targets: parse_targets(get("RESPONSE_TARGETS").unwrap_or("origin")),
            event_targets: parse_targets(get("EVENT_TARGETS").unwrap_or("clients")),
            schema_dir: PathBuf::from(get("SCHEMA_DIR").unwrap_or("schemas")),
            validate_output: get("VALIDATE_OUTPUT").map(is_true).unwrap_or(true),
            default_timeout: ms("DEFAULT_TIMEOUT_MS", 5_000)?,
            step_timeout: ms("STEP_TIMEOUT_MS", 2_000)?,
            queue_capacity: get("QUEUE_CAPACITY").map(str::parse).transpose()?.unwrap_or(100),
            dedup,
            reconnect_delay: ms("RECONNECT_DELAY_MS", 1_000)?,
            default_device_id: get("DEFAULT_DEVICE_ID").unwrap_or("device-1").to_string(),
            disabled_steps: parse_set(get("DISABLED_STEPS")),
            enabled_steps: parse_set(get("ENABLED_STEPS")),
            disabled_triggers: parse_set(get("DISABLED_TRIGGERS")),
            vars,
        })
    }

    /// Valeur brute d'une variable d'environnement (comportements spécifiques).
    pub fn get(&self, key: &str) -> Option<&str> {
        self.vars.get(key).map(|v| v.trim()).filter(|v| !v.is_empty())
    }

    pub fn flag(&self, key: &str) -> bool {
        self.get(key).map(is_true).unwrap_or(false)
    }

    /// Timeout global d'un workflow : `TIMEOUT_<NOM>_MS` sinon `DEFAULT_TIMEOUT_MS`.
    /// Ex. `turnOnDevice` -> `TIMEOUT_TURN_ON_DEVICE_MS`.
    pub fn timeout_for(&self, workflow: &str) -> Duration {
        self.timeout_override(workflow).unwrap_or(self.default_timeout)
    }

    /// `TIMEOUT_<NOM>_MS` s'il est défini.
    pub fn timeout_override(&self, workflow: &str) -> Option<Duration> {
        self.duration(&format!("TIMEOUT_{}_MS", upper_snake(workflow)))
    }

    /// Surcharge du timeout d'une étape : `STEP_TIMEOUT_<WORKFLOW>_<ETAPE>_MS`.
    pub fn step_timeout_for(&self, workflow: &str, step: &str) -> Option<Duration> {
        self.duration(&format!("STEP_TIMEOUT_{}_{}_MS", upper_snake(workflow), upper_snake(step)))
    }

    /// Surcharge de la durée d'une pause : `PAUSE_<WORKFLOW>_<ETAPE>_MS`.
    pub fn pause_for(&self, workflow: &str, step: &str) -> Option<Duration> {
        self.duration(&format!("PAUSE_{}_{}_MS", upper_snake(workflow), upper_snake(step)))
    }

    /// Une étape est active sauf si elle figure dans `DISABLED_STEPS`
    /// (`workflow.etape` ou `*.etape`). Une étape optionnelle doit en plus
    /// être listée dans `ENABLED_STEPS`.
    pub fn step_enabled(&self, workflow: &str, step: &str, optional: bool) -> bool {
        let full = format!("{workflow}.{step}");
        let wildcard = format!("*.{step}");
        let listed = |set: &HashSet<String>| set.contains(&full) || set.contains(&wildcard);
        if listed(&self.disabled_steps) {
            return false;
        }
        !optional || listed(&self.enabled_steps)
    }

    pub fn trigger_enabled(&self, trigger: &str) -> bool {
        !self.disabled_triggers.contains(trigger)
    }

    fn duration(&self, key: &str) -> Option<Duration> {
        self.get(key).and_then(|v| v.parse().ok()).map(Duration::from_millis)
    }
}

fn is_true(v: &str) -> bool {
    matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
}

fn parse_set(v: Option<&str>) -> HashSet<String> {
    v.unwrap_or("").split(',').map(str::trim).filter(|s| !s.is_empty()).map(String::from).collect()
}

/// `a=x,b=y` -> `[(a, x), (b, y)]`
fn parse_named(v: &str) -> anyhow::Result<Vec<(String, String)>> {
    v.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|item| match item.split_once('=') {
            Some((name, value)) if !name.trim().is_empty() && !value.trim().is_empty() => {
                Ok((name.trim().to_string(), value.trim().to_string()))
            }
            _ => bail!("entrée invalide « {item} », format attendu nom=valeur"),
        })
        .collect()
}

fn parse_targets(v: &str) -> Vec<Target> {
    v.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty() && *s != "none")
        .map(|s| match s {
            "origin" => Target::Origin,
            "clients" => Target::Clients,
            name => Target::Server(name.to_string()),
        })
        .collect()
}

/// `turnOnDevice` -> `TURN_ON_DEVICE`, `verify_state` -> `VERIFY_STATE`.
pub fn upper_snake(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    let mut prev_lower = false;
    for c in s.chars() {
        if c.is_ascii_uppercase() && prev_lower {
            out.push('_');
        }
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_uppercase());
        } else {
            out.push('_');
        }
        prev_lower = c.is_ascii_lowercase() || c.is_ascii_digit();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(pairs: &[(&str, &str)]) -> Settings {
        Settings::from_map(pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()).unwrap()
    }

    #[test]
    fn snake_case_conversion() {
        assert_eq!(upper_snake("turnOnDevice"), "TURN_ON_DEVICE");
        assert_eq!(upper_snake("verify_state"), "VERIFY_STATE");
        assert_eq!(upper_snake("onStateChanged"), "ON_STATE_CHANGED");
    }

    #[test]
    fn per_workflow_timeouts() {
        let s = settings(&[("DEFAULT_TIMEOUT_MS", "100"), ("TIMEOUT_TURN_ON_DEVICE_MS", "250")]);
        assert_eq!(s.timeout_for("turnOnDevice"), Duration::from_millis(250));
        assert_eq!(s.timeout_for("turnOffDevice"), Duration::from_millis(100));
    }

    #[test]
    fn step_toggles() {
        let s = settings(&[
            ("DISABLED_STEPS", "turnOnDevice.read_state,*.verify_state"),
            ("ENABLED_STEPS", "turnOnDevice.notify"),
        ]);
        assert!(!s.step_enabled("turnOnDevice", "read_state", false));
        assert!(s.step_enabled("turnOffDevice", "read_state", false));
        assert!(!s.step_enabled("turnOffDevice", "verify_state", false));
        assert!(s.step_enabled("turnOnDevice", "notify", true));
        assert!(!s.step_enabled("turnOffDevice", "notify", true));
    }

    #[test]
    fn targets_and_servers() {
        let s = settings(&[
            ("TCP_SERVERS", "server_1=127.0.0.1:7001, server_2=127.0.0.1:7002"),
            ("EVENT_TARGETS", "clients,server_1"),
        ]);
        assert_eq!(s.tcp_servers.len(), 2);
        assert_eq!(s.event_targets, vec![Target::Clients, Target::Server("server_1".into())]);
        assert_eq!(s.response_targets, vec![Target::Origin]);
    }
}
