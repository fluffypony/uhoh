use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};

use super::AppState;

pub async fn websocket_handler(
    headers: HeaderMap,
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> Result<impl IntoResponse, StatusCode> {
    // Validate Origin header to prevent cross-site WebSocket hijacking
    if let Some(origin) = headers.get("origin") {
        if let Ok(origin_str) = origin.to_str() {
            let allowed = origin_str.starts_with("http://127.0.0.1")
                || origin_str.starts_with("http://localhost")
                || origin_str.starts_with("https://127.0.0.1")
                || origin_str.starts_with("https://localhost");
            if !allowed {
                return Err(StatusCode::FORBIDDEN);
            }
        }
    }

    Ok(ws.on_upgrade(move |socket| handle_socket(socket, state)))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let mut event_rx = state.event_tx.subscribe();

    let send_task = tokio::spawn(async move {
        loop {
            match event_rx.recv().await {
                Ok(event) => {
                    let payload = match serde_json::to_string(&event) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    if sender.send(Message::Text(payload.into())).await.is_err() {
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::debug!("WebSocket client lagged, skipped {} events", n);
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    });

    let recv_task = tokio::spawn(async move {
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(Message::Ping(_)) => {}
                Ok(Message::Close(_)) => break,
                Err(_) => break,
                _ => {}
            }
        }
    });

    tokio::select! {
        _ = send_task => {},
        _ = recv_task => {},
    }
}
