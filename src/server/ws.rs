use axum::{
    extract::{
        ws::{Message, WebSocket},
        Query, State, WebSocketUpgrade,
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use subtle::ConstantTimeEq;

use super::AppState;

#[derive(serde::Deserialize)]
pub struct WsAuthQuery {
    token: Option<String>,
}

pub async fn websocket_handler(
    headers: HeaderMap,
    Query(query): Query<WsAuthQuery>,
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> Result<impl IntoResponse, StatusCode> {
    // Validate Origin header to prevent cross-site WebSocket hijacking
    if let Some(origin) = headers.get("origin") {
        if let Ok(origin_str) = origin.to_str() {
            let allowed = url::Url::parse(origin_str)
                .ok()
                .and_then(|u| u.host_str().map(str::to_string))
                .map(|host| {
                    host.eq_ignore_ascii_case("127.0.0.1")
                        || host.eq_ignore_ascii_case("localhost")
                        || host == "::1"
                })
                .unwrap_or(false);
            if !allowed {
                return Err(StatusCode::FORBIDDEN);
            }
        }
    }

    // If HTTP auth is enabled, require the same bearer token for /ws via
    // Authorization or Sec-WebSocket-Protocol, since /ws is auth-middleware exempt.
    if state.config.server.require_auth {
        if !websocket_auth_ok(&headers, query.token.as_deref(), state.cached_token.as_deref()) {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }

    // Echo matched Sec-WebSocket-Protocol so strict clients accept the connection
    let matched_protocol = extract_matched_protocol(&headers);
    let mut upgrade = ws;
    if let Some(proto) = matched_protocol {
        upgrade = upgrade.protocols([proto]);
    }
    Ok(upgrade.on_upgrade(move |socket| handle_socket(socket, state)))
}

fn websocket_auth_ok(
    headers: &HeaderMap,
    query_token: Option<&str>,
    cached_token: Option<&str>,
) -> bool {
    let expected = match cached_token {
        Some(t) if !t.is_empty() => t,
        _ => return false,
    };

    if let Some(auth_header) = headers.get("authorization") {
        if let Ok(auth) = auth_header.to_str() {
            if let Some(provided) = auth.strip_prefix("Bearer ") {
                if provided.as_bytes().ct_eq(expected.as_bytes()).into() {
                    return true;
                }
            }
        }
    }

    if let Some(token) = query_token {
        if token.as_bytes().ct_eq(expected.as_bytes()).into() {
            return true;
        }
    }

    if let Some(protocol_header) = headers.get("sec-websocket-protocol") {
        if let Ok(protocols) = protocol_header.to_str() {
            // Accept token transport via protocol list, e.g.:
            // Sec-WebSocket-Protocol: bearer, <token>
            let mut iter = protocols.split(',').map(|s| s.trim());
            while let Some(name) = iter.next() {
                if name.eq_ignore_ascii_case("bearer") {
                    if let Some(token) = iter.next() {
                        return token.as_bytes().ct_eq(expected.as_bytes()).into();
                    }
                }
            }

            // Also accept a single token-prefixed protocol value:
            // Sec-WebSocket-Protocol: bearer.<token>
            for value in protocols.split(',').map(|s| s.trim()) {
                if let Some(token) = value.strip_prefix("bearer.") {
                    return token.as_bytes().ct_eq(expected.as_bytes()).into();
                }
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
        if name.eq_ignore_ascii_case("bearer") {
            if iter.next().is_some() {
                return Some("bearer".to_string());
            }
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
