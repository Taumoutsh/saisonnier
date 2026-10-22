//! Driver WebSocket client (texte JSON), avec reconnexion automatique.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use super::{DriverIo, Interface, channel};

pub fn spawn(name: &str, url: &str, reconnect_delay: Duration) -> Interface {
    let (iface, io) = channel(name, "websocket");
    tokio::spawn(run(name.to_string(), url.to_string(), reconnect_delay, io));
    iface
}

async fn run(name: String, url: String, reconnect_delay: Duration, mut io: DriverIo) {
    loop {
        match connect_async(url.as_str()).await {
            Ok((stream, _)) => {
                info!(interface = %name, %url, "WebSocket connecté");
                let (mut sink, mut source) = stream.split();
                io.connected.send_replace(true);
                loop {
                    tokio::select! {
                        out = io.outgoing.recv() => {
                            let Some(msg) = out else { return };
                            debug!(interface = %name, %msg, "-> ws");
                            if let Err(e) = sink.send(Message::text(msg.to_string())).await {
                                warn!(interface = %name, error = %e, "échec d'envoi WebSocket");
                                break;
                            }
                        }
                        frame = source.next() => match frame {
                            Some(Ok(Message::Text(text))) => publish(&name, &io, text.as_bytes()),
                            Some(Ok(Message::Binary(bytes))) => publish(&name, &io, &bytes),
                            Some(Ok(Message::Close(_))) | None => {
                                info!(interface = %name, "WebSocket fermé par le serveur");
                                break;
                            }
                            Some(Ok(_)) => {}
                            Some(Err(e)) => {
                                warn!(interface = %name, error = %e, "erreur WebSocket");
                                break;
                            }
                        }
                    }
                }
                io.connected.send_replace(false);
            }
            Err(e) => debug!(interface = %name, %url, error = %e, "connexion WebSocket impossible"),
        }
        tokio::time::sleep(reconnect_delay).await;
    }
}

fn publish(name: &str, io: &DriverIo, raw: &[u8]) {
    match serde_json::from_slice::<Value>(raw) {
        Ok(msg) => {
            debug!(interface = %name, %msg, "<- ws");
            let _ = io.incoming.send(msg);
        }
        Err(e) => warn!(interface = %name, error = %e, "message WebSocket non JSON ignoré"),
    }
}
