//! Catalogue : association requête -> workflow, et déclencheurs sur
//! messages spontanés. C'est ici qu'on ajoute / retire des actions.

pub mod device;

use crate::config::Settings;
use crate::engine::Trigger;
use crate::engine::workflow::Workflow;
use crate::model::{Command, InboundRequest};

pub fn plan(req: &InboundRequest, s: &Settings) -> Workflow {
    let name = req.name();
    let device = |id: &Option<String>| id.clone().unwrap_or_else(|| s.default_device_id.clone());
    match &req.command {
        Command::TurnOnDevice(p) => device::power(s, name, &device(&p.device_id), true),
        Command::TurnOffDevice(p) => device::power(s, name, &device(&p.device_id), false),
        Command::GetDeviceStatus(p) => device::status(s, name, &device(&p.device_id)),
        Command::SetDeviceLevel(p) => device::set_level(s, name, &device(&p.device_id), p.level),
        Command::RebootDevice(p) => device::reboot(s, name, &device(&p.device_id)),
    }
}

/// Déclencheurs actifs (désactivables par `DISABLED_TRIGGERS`).
pub fn triggers(s: &Settings) -> Vec<Trigger> {
    let iface = device::interface(s);
    vec![
        Trigger {
            name: "onStateChanged".into(),
            interface: iface.clone(),
            matches: Box::new(|m| device::is_notification(m, "stateChanged")),
            plan: Box::new(device::on_state_changed),
        },
        Trigger {
            name: "onAlarm".into(),
            interface: iface,
            matches: Box::new(|m| device::is_notification(m, "alarm")),
            plan: Box::new(device::on_alarm),
        },
    ]
    .into_iter()
    .filter(|t| s.trigger_enabled(&t.name))
    .collect()
}
