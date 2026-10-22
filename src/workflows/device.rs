//! Workflows de pilotage de l'équipement, via l'interface WebSocket.
//!
//! Protocole WebSocket avec l'équipement (voir aussi `tools/ws_simulator.py`) :
//! - commande : `{"id": "<corrélation>", "action": "powerOn", "deviceId": "lamp-1", ...}`
//! - retour   : `{"id": "<corrélation>", "status": "OK" | "ERROR", "data": {...}, "error": "..."}`
//! - spontané : `{"type": "notification", "notification": "stateChanged" | "ready" | "alarm", "deviceId": ..., ...}`

use serde_json::{Value, json};

use crate::config::Settings;
use crate::engine::workflow::{Action, Ctx, Step, Workflow, WorkflowBuilder};

/// Nom de l'interface de l'équipement (`DEVICE_INTERFACE`, défaut `device`).
pub fn interface(s: &Settings) -> String {
    s.get("DEVICE_INTERFACE").unwrap_or("device").to_string()
}

/// Commande envoyée à l'équipement, avec attente du retour corrélé.
pub fn call(iface: &str, action: &'static str, device_id: &str, extra: Value) -> Action {
    let device_id = device_id.to_string();
    Action::request(iface, move |_| {
        let mut msg = json!({ "action": action, "deviceId": device_id });
        if let (Value::Object(m), Value::Object(e)) = (&mut msg, &extra) {
            m.extend(e.clone());
        }
        msg
    })
}

/// Contrôle : le retour de l'équipement doit avoir `status == "OK"`.
pub fn expect_ok(_: &Ctx, reply: &Value) -> Result<(), String> {
    match reply.get("status").and_then(Value::as_str) {
        Some("OK") => Ok(()),
        _ => Err(format!(
            "refus de l'équipement : {}",
            reply.get("error").and_then(Value::as_str).unwrap_or("status != OK")
        )),
    }
}

fn expect_field(field: &'static str, expected: Value) -> impl Fn(&Ctx, &Value) -> Result<(), String> {
    move |_, reply| match reply.pointer(&format!("/data/{field}")) {
        Some(v) if *v == expected => Ok(()),
        other => Err(format!("{field} attendu {expected}, obtenu {}", other.unwrap_or(&Value::Null))),
    }
}

pub fn is_notification(msg: &Value, kind: &str) -> bool {
    msg.get("type").and_then(Value::as_str) == Some("notification")
        && msg.get("notification").and_then(Value::as_str) == Some(kind)
}

/// `turnOnDevice` / `turnOffDevice`.
pub fn power(s: &Settings, name: &str, device_id: &str, on: bool) -> Workflow {
    let (target, command, event) =
        if on { ("ON", "powerOn", "deviceTurnedOn") } else { ("OFF", "powerOff", "deviceTurnedOff") };
    let iface = interface(s);
    let (dev_event, dev_result) = (device_id.to_string(), device_id.to_string());

    WorkflowBuilder::new(name, s)
        .then(
            Step::new("read_state", call(&iface, "getState", device_id, json!({})))
                .check(expect_ok)
                .store_as("initial"),
        )
        .then(
            Step::new("power", call(&iface, command, device_id, json!({})))
                .check(expect_ok)
                .when(move |ctx| ctx.get_str("/initial/data/state") != Some(target)),
        )
        .then(Step::new("settle", Action::pause_ms(0)))
        .then(
            Step::new("verify_state", call(&iface, "getState", device_id, json!({})))
                .check(expect_ok)
                .check(expect_field("state", json!(target)))
                .store_as("final"),
        )
        .then_optional(Step::new(
            "notify",
            Action::emit(event, move |_| json!({ "deviceId": dev_event, "state": target })),
        ))
        .result(move |ctx| {
            json!({
                "deviceId": dev_result,
                "state": ctx.get_str("/final/data/state").unwrap_or(target),
                "alreadyInState": ctx.get_str("/initial/data/state") == Some(target),
            })
        })
        .build()
}

/// `getDeviceStatus`.
pub fn status(s: &Settings, name: &str, device_id: &str) -> Workflow {
    let iface = interface(s);
    let dev = device_id.to_string();
    WorkflowBuilder::new(name, s)
        .then(
            Step::new("read_state", call(&iface, "getState", device_id, json!({}))).check(expect_ok).store_as("status"),
        )
        .result(move |ctx| {
            let mut data = ctx.get("/status/data").cloned().unwrap_or_else(|| json!({}));
            if let Value::Object(m) = &mut data {
                m.insert("deviceId".into(), json!(dev));
            }
            data
        })
        .build()
}

/// `setDeviceLevel`.
pub fn set_level(s: &Settings, name: &str, device_id: &str, level: u8) -> Workflow {
    let iface = interface(s);
    let dev = device_id.to_string();
    WorkflowBuilder::new(name, s)
        .then(Step::new("set_level", call(&iface, "setLevel", device_id, json!({ "level": level }))).check(expect_ok))
        .then(
            Step::new("verify_level", call(&iface, "getState", device_id, json!({})))
                .check(expect_ok)
                .check(expect_field("level", json!(level)))
                .store_as("final"),
        )
        .result(move |ctx| {
            json!({ "deviceId": dev, "level": ctx.get("/final/data/level").cloned().unwrap_or(json!(level)) })
        })
        .build()
}

/// `rebootDevice` : illustre l'attente d'un message spontané, les pauses et
/// l'appel à une autre interface (accord de maintenance, optionnel).
pub fn reboot(s: &Settings, name: &str, device_id: &str) -> Workflow {
    let iface = interface(s);
    let ack_iface = s.get("MAINTENANCE_INTERFACE").unwrap_or("server_2").to_string();
    let (dev_ack, dev_ready, dev_event, dev_result) =
        (device_id.to_string(), device_id.to_string(), device_id.to_string(), device_id.to_string());

    WorkflowBuilder::new(name, s)
        .default_timeout_ms(15_000)
        .then_optional(
            Step::new(
                "maintenance_ack",
                Action::request(&ack_iface, move |_| json!({ "maintenance": "reboot", "deviceId": dev_ack })),
            )
            .check(expect_ok),
        )
        .then(Step::new("reboot", call(&iface, "reboot", device_id, json!({}))).check(expect_ok))
        .then(
            Step::new(
                "wait_ready",
                Action::wait_for(&iface, move |_, msg| {
                    is_notification(msg, "ready") && msg.get("deviceId").and_then(Value::as_str) == Some(&dev_ready)
                }),
            )
            .timeout_ms(10_000),
        )
        .then(Step::new("settle", Action::pause_ms(0)))
        .then(
            Step::new("verify_state", call(&iface, "getState", device_id, json!({})))
                .check(expect_ok)
                .store_as("final"),
        )
        .then_optional(Step::new(
            "notify",
            Action::emit("deviceRebooted", move |_| json!({ "deviceId": dev_event })),
        ))
        .result(move |ctx| {
            json!({ "deviceId": dev_result, "state": ctx.get("/final/data/state").cloned().unwrap_or(Value::Null) })
        })
        .build()
}

/// Déclencheur `onStateChanged` : notification spontanée de changement
/// d'état -> relecture de l'état -> évènement `deviceStateChanged`.
pub fn on_state_changed(trigger: &Value, s: &Settings) -> Workflow {
    let iface = interface(s);
    let dev = trigger.get("deviceId").and_then(Value::as_str).unwrap_or(&s.default_device_id).to_string();
    let dev_event = dev.clone();
    WorkflowBuilder::new("onStateChanged", s)
        .then(
            Step::new("refresh", call(&iface, "getState", &dev, json!({})))
                .check(expect_ok)
                .store_as("status")
                .continue_on_error(),
        )
        .then(Step::new(
            "notify",
            Action::emit("deviceStateChanged", move |ctx| {
                json!({
                    "deviceId": dev_event,
                    "state": ctx.get("/status/data/state").or(ctx.get("/trigger/state")).cloned().unwrap_or(Value::Null),
                    "level": ctx.get("/status/data/level").cloned().unwrap_or(Value::Null),
                })
            }),
        ))
        .build()
}

/// Déclencheur `onAlarm` : alarme spontanée -> évènement `deviceAlarm`.
pub fn on_alarm(trigger: &Value, s: &Settings) -> Workflow {
    let dev = trigger.get("deviceId").and_then(Value::as_str).unwrap_or(&s.default_device_id).to_string();
    WorkflowBuilder::new("onAlarm", s)
        .then(Step::new(
            "notify",
            Action::emit("deviceAlarm", move |ctx| {
                json!({
                    "deviceId": dev,
                    "alarm": ctx.get("/trigger/alarm").cloned().unwrap_or(Value::Null),
                    "severity": ctx.get("/trigger/severity").cloned().unwrap_or(json!("warning")),
                })
            }),
        ))
        .build()
}
