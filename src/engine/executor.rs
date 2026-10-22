//! Exécution séquentielle des étapes d'un workflow.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::time::timeout;
use tracing::{debug, warn};

use super::Runtime;
use super::workflow::{Action, CORRELATION_FIELD, Ctx, Step, Workflow};
use crate::interfaces::{Interface, InterfaceError, Interfaces};
use crate::model::{ErrorCode, ErrorInfo, Event};

/// Nombre maximal de messages reçus mais non consommés conservés par workflow.
const BACKLOG_CAPACITY: usize = 256;

static CORRELATION_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct Failure {
    pub code: ErrorCode,
    pub message: String,
    pub step: Option<String>,
}

impl From<Failure> for ErrorInfo {
    fn from(f: Failure) -> Self {
        ErrorInfo { code: f.code, message: f.message, step: f.step }
    }
}

/// Exécute le workflow dans la limite de son timeout global et renvoie le
/// `data` de la réponse.
pub async fn run(wf: &Workflow, ctx: &mut Ctx, rt: &Runtime) -> Result<Value, Failure> {
    let mut mailbox = Mailbox::new(&rt.interfaces);
    match timeout(wf.timeout, run_steps(wf, ctx, rt, &mut mailbox)).await {
        Ok(result) => result,
        Err(_) => Err(Failure {
            code: ErrorCode::Timeout,
            message: format!("timeout global de {} ms dépassé", wf.timeout.as_millis()),
            step: ctx.current_step.clone(),
        }),
    }
}

async fn run_steps(wf: &Workflow, ctx: &mut Ctx, rt: &Runtime, mailbox: &mut Mailbox) -> Result<Value, Failure> {
    for step in &wf.steps {
        if let Some(when) = &step.when
            && !when(ctx)
        {
            debug!(workflow = %wf.name, step = %step.id, "étape ignorée (condition fausse)");
            continue;
        }
        ctx.current_step = Some(step.id.clone());
        debug!(workflow = %wf.name, step = %step.id, action = step.action.kind(), "étape");
        if let Err(failure) = run_step(step, ctx, rt, mailbox).await {
            if step.continue_on_error {
                warn!(workflow = %wf.name, step = %step.id, error = %failure.message, "échec ignoré");
                continue;
            }
            return Err(failure);
        }
    }
    ctx.current_step = None;
    Ok(wf.result.as_ref().map(|f| f(ctx)).unwrap_or_else(|| Value::Object(Default::default())))
}

async fn run_step(step: &Step, ctx: &mut Ctx, rt: &Runtime, mailbox: &mut Mailbox) -> Result<(), Failure> {
    let fail = |code, message: String| Failure { code, message, step: Some(step.id.clone()) };
    let wait_limit = step.timeout.unwrap_or(rt.settings.step_timeout);

    let received = match &step.action {
        Action::Send { interface, message } => {
            let iface = lookup(&rt.interfaces, interface).map_err(|m| fail(ErrorCode::InterfaceUnavailable, m))?;
            iface.send(message(ctx)).map_err(|e| fail(interface_code(&e), e.to_string()))?;
            None
        }
        Action::Request { interface, message } => {
            let iface = lookup(&rt.interfaces, interface).map_err(|m| fail(ErrorCode::InterfaceUnavailable, m))?;
            let corr = format!("{}:{}:{}", ctx.source_id, step.id, CORRELATION_SEQ.fetch_add(1, Ordering::Relaxed));
            let mut msg = message(ctx);
            if let Value::Object(map) = &mut msg {
                map.insert(CORRELATION_FIELD.to_string(), Value::String(corr.clone()));
            }
            iface.send(msg).map_err(|e| fail(interface_code(&e), e.to_string()))?;
            let matches = |v: &Value| v.get(CORRELATION_FIELD).and_then(Value::as_str) == Some(corr.as_str());
            let reply = wait(mailbox, interface, matches, wait_limit)
                .await
                .map_err(|(code, m)| fail(code, format!("pas de réponse de « {interface} » : {m}")))?;
            Some(reply)
        }
        Action::WaitFor { interface, filter } => {
            let snapshot: &Ctx = ctx;
            let msg = wait(mailbox, interface, |v| filter(snapshot, v), wait_limit)
                .await
                .map_err(|(code, m)| fail(code, format!("message attendu sur « {interface} » non reçu : {m}")))?;
            Some(msg)
        }
        Action::Pause(duration) => {
            tokio::time::sleep(*duration).await;
            None
        }
        Action::Emit { event, data } => {
            rt.router.send_event(&Event::new(event, Some(ctx.source_id.clone()), data(ctx)));
            None
        }
        Action::Local(f) => {
            f(ctx).map_err(|m| fail(ErrorCode::StepFailed, m))?;
            None
        }
    };

    if let Some(msg) = received {
        if let Some(var) = &step.store_as {
            ctx.set(var, msg.clone());
        }
        for check in &step.checks {
            check(ctx, &msg).map_err(|m| fail(ErrorCode::StepFailed, m))?;
        }
    }
    Ok(())
}

async fn wait(
    mailbox: &mut Mailbox,
    interface: &str,
    matches: impl FnMut(&Value) -> bool,
    limit: Duration,
) -> Result<Value, (ErrorCode, String)> {
    match timeout(limit, mailbox.next_matching(interface, matches)).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(m)) => Err((ErrorCode::InterfaceUnavailable, m)),
        Err(_) => Err((ErrorCode::Timeout, format!("timeout de {} ms", limit.as_millis()))),
    }
}

fn lookup<'a>(interfaces: &'a Interfaces, name: &str) -> Result<&'a Interface, String> {
    interfaces.get(name).ok_or_else(|| format!("interface inconnue : « {name} »"))
}

fn interface_code(e: &InterfaceError) -> ErrorCode {
    match e {
        InterfaceError::NotConnected(_) | InterfaceError::Closed(_) => ErrorCode::InterfaceUnavailable,
        InterfaceError::Busy(_) => ErrorCode::Internal,
    }
}

/// Boîte aux lettres d'un workflow : abonnée à toutes les interfaces dès le
/// début du workflow, elle garantit qu'un message arrivé avant l'étape qui
/// l'attend (réponse très rapide, notification anticipée) n'est pas perdu.
struct Mailbox {
    receivers: HashMap<String, broadcast::Receiver<Value>>,
    backlog: VecDeque<(String, Value)>,
}

impl Mailbox {
    fn new(interfaces: &Interfaces) -> Self {
        Self {
            receivers: interfaces.iter().map(|i| (i.name().to_string(), i.subscribe())).collect(),
            backlog: VecDeque::new(),
        }
    }

    async fn next_matching(
        &mut self,
        interface: &str,
        mut matches: impl FnMut(&Value) -> bool,
    ) -> Result<Value, String> {
        if let Some(pos) = self.backlog.iter().position(|(i, v)| i == interface && matches(v)) {
            return Ok(self.backlog.remove(pos).expect("position valide").1);
        }
        let rx = self.receivers.get_mut(interface).ok_or_else(|| format!("interface inconnue : « {interface} »"))?;
        loop {
            match rx.recv().await {
                Ok(v) if matches(&v) => return Ok(v),
                Ok(v) => {
                    if self.backlog.len() == BACKLOG_CAPACITY {
                        self.backlog.pop_front();
                    }
                    self.backlog.push_back((interface.to_string(), v));
                }
                Err(RecvError::Lagged(n)) => warn!(%interface, skipped = n, "messages entrants perdus (retard)"),
                Err(RecvError::Closed) => return Err(format!("interface « {interface} » arrêtée")),
            }
        }
    }
}
