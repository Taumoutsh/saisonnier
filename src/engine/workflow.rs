//! Modèle des workflows : une liste ordonnée d'étapes ([`Step`]), chacune
//! portant une [`Action`]. Les workflows sont construits par code (voir
//! `crate::workflows`) via [`WorkflowBuilder`], qui applique les
//! activations / désactivations et surcharges venant de l'environnement.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value};

use crate::config::Settings;

pub type MessageBuilder = Arc<dyn Fn(&Ctx) -> Value + Send + Sync>;
pub type Predicate = Arc<dyn Fn(&Ctx) -> bool + Send + Sync>;
pub type MessageFilter = Arc<dyn Fn(&Ctx, &Value) -> bool + Send + Sync>;
pub type Check = Arc<dyn Fn(&Ctx, &Value) -> Result<(), String> + Send + Sync>;
pub type LocalFn = Arc<dyn Fn(&mut Ctx) -> Result<(), String> + Send + Sync>;

/// Champ portant l'identifiant de corrélation dans les messages
/// [`Action::Request`] (ajouté à l'envoi, attendu dans la réponse).
pub const CORRELATION_FIELD: &str = "id";

/// Les types d'actions disponibles pour une étape.
#[derive(Clone)]
pub enum Action {
    /// Envoie un message sur une interface, sans attendre de retour.
    Send { interface: String, message: MessageBuilder },
    /// Envoie un message avec un identifiant de corrélation et attend le
    /// message de retour portant le même identifiant.
    Request { interface: String, message: MessageBuilder },
    /// Attend un message spontané correspondant au filtre (reçu depuis le
    /// début du workflow), sur n'importe quelle interface.
    WaitFor { interface: String, filter: MessageFilter },
    /// Pause (surchargeable par `PAUSE_<WORKFLOW>_<ETAPE>_MS`).
    Pause(Duration),
    /// Émet un évènement vers `EVENT_TARGETS`.
    Emit { event: String, data: MessageBuilder },
    /// Traitement local sur le contexte (calcul, contrôle, ...).
    Local(LocalFn),
}

impl Action {
    pub fn send(interface: &str, message: impl Fn(&Ctx) -> Value + Send + Sync + 'static) -> Self {
        Action::Send { interface: interface.to_string(), message: Arc::new(message) }
    }

    pub fn request(interface: &str, message: impl Fn(&Ctx) -> Value + Send + Sync + 'static) -> Self {
        Action::Request { interface: interface.to_string(), message: Arc::new(message) }
    }

    pub fn wait_for(interface: &str, filter: impl Fn(&Ctx, &Value) -> bool + Send + Sync + 'static) -> Self {
        Action::WaitFor { interface: interface.to_string(), filter: Arc::new(filter) }
    }

    pub fn pause_ms(ms: u64) -> Self {
        Action::Pause(Duration::from_millis(ms))
    }

    pub fn emit(event: &str, data: impl Fn(&Ctx) -> Value + Send + Sync + 'static) -> Self {
        Action::Emit { event: event.to_string(), data: Arc::new(data) }
    }

    pub fn local(f: impl Fn(&mut Ctx) -> Result<(), String> + Send + Sync + 'static) -> Self {
        Action::Local(Arc::new(f))
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Action::Send { .. } => "send",
            Action::Request { .. } => "request",
            Action::WaitFor { .. } => "wait_for",
            Action::Pause(_) => "pause",
            Action::Emit { .. } => "emit",
            Action::Local(_) => "local",
        }
    }
}

#[derive(Clone)]
pub struct Step {
    pub id: String,
    pub action: Action,
    /// Timeout de l'attente (Request / WaitFor). Défaut : `STEP_TIMEOUT_MS`.
    pub timeout: Option<Duration>,
    /// Condition d'exécution évaluée juste avant l'étape.
    pub when: Option<Predicate>,
    /// Contrôles appliqués au message reçu (Request / WaitFor).
    pub checks: Vec<Check>,
    /// Nom de la variable de contexte où stocker le message reçu.
    pub store_as: Option<String>,
    /// Si vrai, un échec est journalisé mais n'interrompt pas le workflow.
    pub continue_on_error: bool,
}

impl Step {
    pub fn new(id: &str, action: Action) -> Self {
        Self {
            id: id.to_string(),
            action,
            timeout: None,
            when: None,
            checks: Vec::new(),
            store_as: None,
            continue_on_error: false,
        }
    }

    pub fn timeout_ms(mut self, ms: u64) -> Self {
        self.timeout = Some(Duration::from_millis(ms));
        self
    }

    pub fn when(mut self, f: impl Fn(&Ctx) -> bool + Send + Sync + 'static) -> Self {
        self.when = Some(Arc::new(f));
        self
    }

    pub fn check(mut self, f: impl Fn(&Ctx, &Value) -> Result<(), String> + Send + Sync + 'static) -> Self {
        self.checks.push(Arc::new(f));
        self
    }

    pub fn store_as(mut self, var: &str) -> Self {
        self.store_as = Some(var.to_string());
        self
    }

    pub fn continue_on_error(mut self) -> Self {
        self.continue_on_error = true;
        self
    }
}

pub struct Workflow {
    pub name: String,
    pub timeout: Duration,
    pub steps: Vec<Step>,
    /// Construit le champ `data` de la réponse à partir du contexte final.
    pub result: Option<MessageBuilder>,
}

/// Construit un workflow en tenant compte de la configuration :
/// - `DISABLED_STEPS` / `ENABLED_STEPS` (étapes retirées / optionnelles ajoutées),
/// - `STEP_TIMEOUT_<WF>_<ETAPE>_MS`, `PAUSE_<WF>_<ETAPE>_MS`,
/// - `TIMEOUT_<WF>_MS`.
pub struct WorkflowBuilder<'a> {
    settings: &'a Settings,
    workflow: Workflow,
}

impl<'a> WorkflowBuilder<'a> {
    pub fn new(name: &str, settings: &'a Settings) -> Self {
        Self {
            settings,
            workflow: Workflow {
                name: name.to_string(),
                timeout: settings.timeout_for(name),
                steps: Vec::new(),
                result: None,
            },
        }
    }

    /// Timeout global par défaut propre à ce workflow (prioritaire sur
    /// `DEFAULT_TIMEOUT_MS`, mais pas sur `TIMEOUT_<WF>_MS`).
    pub fn default_timeout_ms(mut self, ms: u64) -> Self {
        if self.settings.timeout_override(&self.workflow.name).is_none() {
            self.workflow.timeout = Duration::from_millis(ms);
        }
        self
    }

    /// Étape active par défaut (désactivable par `DISABLED_STEPS`).
    pub fn then(self, step: Step) -> Self {
        self.add(step, false)
    }

    /// Étape inactive par défaut (activable par `ENABLED_STEPS`).
    pub fn then_optional(self, step: Step) -> Self {
        self.add(step, true)
    }

    pub fn result(mut self, f: impl Fn(&Ctx) -> Value + Send + Sync + 'static) -> Self {
        self.workflow.result = Some(Arc::new(f));
        self
    }

    pub fn build(self) -> Workflow {
        self.workflow
    }

    fn add(mut self, mut step: Step, optional: bool) -> Self {
        let wf = &self.workflow.name;
        if !self.settings.step_enabled(wf, &step.id, optional) {
            return self;
        }
        if let Some(t) = self.settings.step_timeout_for(wf, &step.id) {
            step.timeout = Some(t);
        }
        if let Action::Pause(d) = &mut step.action
            && let Some(p) = self.settings.pause_for(wf, &step.id)
        {
            *d = p;
        }
        self.workflow.steps.push(step);
        self
    }
}

/// Contexte d'exécution (modèle interne de travail) d'un workflow : il
/// accumule les messages reçus pour construire réponse et évènements.
#[derive(Debug, Clone)]
pub struct Ctx {
    /// Identifiant de la requête, ou du déclencheur.
    pub source_id: String,
    pub workflow: String,
    /// Variables : messages stockés (`store_as`), message déclencheur
    /// (`trigger`), valeurs calculées par les actions locales.
    pub vars: Value,
    pub current_step: Option<String>,
}

impl Ctx {
    pub fn new(source_id: &str, workflow: &str) -> Self {
        Self {
            source_id: source_id.to_string(),
            workflow: workflow.to_string(),
            vars: Value::Object(Map::new()),
            current_step: None,
        }
    }

    pub fn with_var(mut self, key: &str, value: Value) -> Self {
        self.set(key, value);
        self
    }

    pub fn set(&mut self, key: &str, value: Value) {
        if let Value::Object(map) = &mut self.vars {
            map.insert(key.to_string(), value);
        }
    }

    /// Lecture par JSON Pointer, ex. `ctx.get("/final/data/state")`.
    pub fn get(&self, pointer: &str) -> Option<&Value> {
        self.vars.pointer(pointer)
    }

    pub fn get_str(&self, pointer: &str) -> Option<&str> {
        self.get(pointer).and_then(Value::as_str)
    }
}
