//! Driver client TCP (JSON délimité par des retours à la ligne), avec
//! reconnexion automatique. Utilisé pour les serveurs TCP sortants
//! (`TCP_SERVERS`) : les messages en file sont conservés pendant une coupure.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{debug, info, warn};

use super::{DriverIo, Interface, channel};

pub const MAX_LINE_LENGTH: usize = 1024 * 1024;

pub fn spawn(name: &str, addr: &str, reconnect_delay: Duration) -> Interface {
    let (iface, io) = channel(name, "tcp");
    tokio::spawn(run(name.to_string(), addr.to_string(), reconnect_delay, io));
    iface
}

async fn run(name: String, addr: String, reconnect_delay: Duration, mut io: DriverIo) {
    // Message dont l'envoi a échoué : renvoyé en priorité après reconnexion.
    let mut pending: Option<Value> = None;
    loop {
        match TcpStream::connect(&addr).await {
            Ok(stream) => {
                info!(interface = %name, %addr, "serveur TCP connecté");
                let mut framed = Framed::new(stream, LinesCodec::new_with_max_length(MAX_LINE_LENGTH));
                io.connected.send_replace(true);
                loop {
                    let msg = match pending.take() {
                        Some(msg) => msg,
                        None => tokio::select! {
                            out = io.outgoing.recv() => match out {
                                Some(msg) => msg,
                                None => return,
                            },
                            line = framed.next() => {
                                match line {
                                    Some(Ok(line)) => publish(&name, &io, &line),
                                    Some(Err(e)) => {
                                        warn!(interface = %name, error = %e, "erreur de lecture TCP");
                                        break;
                                    }
                                    None => {
                                        info!(interface = %name, "connexion fermée par le serveur TCP");
                                        break;
                                    }
                                }
                                continue;
                            }
                        },
                    };
                    debug!(interface = %name, %msg, "-> tcp");
                    if let Err(e) = framed.send(msg.to_string()).await {
                        warn!(interface = %name, error = %e, "échec d'envoi TCP, nouvel essai après reconnexion");
                        pending = Some(msg);
                        break;
                    }
                }
                io.connected.send_replace(false);
            }
            Err(e) => debug!(interface = %name, %addr, error = %e, "connexion TCP impossible"),
        }
        tokio::time::sleep(reconnect_delay).await;
    }
}

fn publish(name: &str, io: &DriverIo, line: &str) {
    if line.trim().is_empty() {
        return;
    }
    match serde_json::from_str::<Value>(line) {
        Ok(msg) => {
            debug!(interface = %name, %msg, "<- tcp");
            let _ = io.incoming.send(msg);
        }
        Err(e) => warn!(interface = %name, error = %e, "ligne TCP non JSON ignorée"),
    }
}
