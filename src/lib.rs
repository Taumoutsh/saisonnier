//! Passerelle TCP <-> WebSocket pilotée par workflows.

pub mod config;
pub mod engine;
pub mod interfaces;
pub mod model;
pub mod router;
pub mod schema;
pub mod server;
pub mod workflows;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use tokio::net::TcpListener;
use tracing::info;

use config::Settings;
use engine::{Engine, Runtime};
use interfaces::{Interfaces, tcp, websocket};
use router::Router;
use schema::Schemas;
use server::{Clients, Inbound};

/// Application démarrée.
pub struct App {
    pub local_addr: SocketAddr,
    pub interfaces: Interfaces,
    pub clients: Arc<Clients>,
}

/// Démarre interfaces, moteur et serveur TCP d'entrée.
pub async fn start(settings: Settings) -> anyhow::Result<App> {
    let settings = Arc::new(settings);
    let schemas = Arc::new(Schemas::load(&settings.schema_dir)?);

    let mut interfaces = Interfaces::default();
    for (name, url) in &settings.ws_endpoints {
        interfaces.insert(websocket::spawn(name, url, settings.reconnect_delay));
    }
    for (name, addr) in &settings.tcp_servers {
        interfaces.insert(tcp::spawn(name, addr, settings.reconnect_delay));
    }

    let clients = Arc::new(Clients::default());
    let router = Arc::new(Router::new(&settings, clients.clone(), interfaces.clone(), schemas.clone()));
    let runtime =
        Arc::new(Runtime { settings: settings.clone(), interfaces: interfaces.clone(), router: router.clone() });
    let engine = Engine::start(runtime, workflows::plan);
    engine.listen_triggers(&interfaces, workflows::triggers(&settings));

    let listener = TcpListener::bind(&settings.listen_addr)
        .await
        .with_context(|| format!("écoute impossible sur {}", settings.listen_addr))?;
    let local_addr = listener.local_addr()?;
    info!(%local_addr, "serveur TCP en écoute");
    let inbound = Arc::new(Inbound { clients: clients.clone(), schemas, engine, router });
    tokio::spawn(server::serve(listener, inbound));

    Ok(App { local_addr, interfaces, clients })
}
