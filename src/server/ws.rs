use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use subtle::ConstantTimeEq;

use super::AppState;

pub async fn websocket_handler(
    headers: HeaderMap,
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> Result<impl IntoResponse, StatusCode> {
    // Validate Origin header to prevent cross-site WebSocket hijacking.
    // Shared middleware also validates Origin; keep this handler check to
    // preserve behavior when ws route wiring changes.
    if !super::auth::validate_origin(&headers) {
        return Err(StatusCode::FORBIDDEN);
    }

    // If HTTP auth is enabled, require the same bearer token for /ws via
    // Authorization or Sec-WebSocket-Protocol, since /ws is auth-middleware exempt.
    if state.config.server.require_auth
        && !websocket_auth_ok(&headers, state.cached_token.as_deref())
    {
        return Err(StatusCode::UNAUTHORIZED);
    }

    // Echo matched Sec-WebSocket-Protocol so strict clients accept the connection
    let matched_protocol = extract_matched_protocol(&headers);
    let mut upgrade = ws;
    if let Some(proto) = matched_protocol {
        upgrade = upgrade.protocols([proto]);
    }
    Ok(upgrade.on_upgrade(move |socket| handle_socket(socket, state)))
}

fn websocket_auth_ok(headers: &HeaderMap, cached_token: Option<&str>) -> bool {
    let expected = match cached_token {
        Some(t) if !t.is_empty() => t,
        _ => return false,
    };

    // Authorization: Bearer <token>
    if let Some(auth_header) = headers.get("authorization") {
        if let Ok(auth) = auth_header.to_str() {
            if let Some(provided) = auth.strip_prefix("Bearer ") {
                if provided.as_bytes().ct_eq(expected.as_bytes()).into() {
                    return true;
                }
            }
        }
    }

    // Sec-WebSocket-Protocol: bearer, <token>
    if let Some(protocol_header) = headers.get("sec-websocket-protocol") {
        if let Ok(protocols) = protocol_header.to_str() {
            if let Some(token) = extract_protocol_token(protocols) {
                return token.as_bytes().ct_eq(expected.as_bytes()).into();
            }
        }
    }

    false
}

/// Extract the protocol value to echo back to the client when auth was via Sec-WebSocket-Protocol.
fn extract_matched_protocol(headers: &HeaderMap) -> Option<String> {
    let protocol_header = headers.get("sec-websocket-protocol")?;
    let protocols = protocol_header.to_str().ok()?;
    // Check "bearer, <token>" format
    let mut iter = protocols.split(',').map(|s| s.trim());
    while let Some(name) = iter.next() {
        if name.eq_ignore_ascii_case("bearer") && iter.next().is_some() {
            return Some("bearer".to_string());
        }
    }
    // Check "bearer.<token>" format
    for value in protocols.split(',').map(|s| s.trim()) {
        if value.starts_with("bearer.") {
            return Some(value.to_string());
        }
    }
    None
}

fn extract_protocol_token(protocols: &str) -> Option<&str> {
    let mut iter = protocols.split(',').map(|s| s.trim());
    while let Some(name) = iter.next() {
        if name.eq_ignore_ascii_case("bearer") {
            if let Some(token) = iter.next() {
                return Some(token);
            }
        }
    }
    for value in protocols.split(',').map(|s| s.trim()) {
        if let Some(token) = value.strip_prefix("bearer.") {
            if !token.is_empty() {
                return Some(token);
            }
        }
    }
    None
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
