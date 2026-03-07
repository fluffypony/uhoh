use axum::{
    extract::Request,
    extract::State,
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use rand::Rng;
use std::fs;
use std::path::{Path, PathBuf};
use subtle::ConstantTimeEq;

#[derive(Clone)]
pub struct AuthToken(pub String);

#[derive(Clone, Copy)]
pub struct AuthConfig {
    pub require_auth: bool,
    pub mcp_require_auth: bool,
}

pub fn generate_token() -> String {
    let mut rng = rand::thread_rng();
    let bytes: [u8; 32] = rng.gen();
    hex::encode(bytes)
}

pub fn write_token_file(uhoh_dir: &Path, token: &str) -> anyhow::Result<PathBuf> {
    let token_path = uhoh_dir.join("server.token");
    let tmp_path = uhoh_dir.join(".server.token.tmp");
    fs::write(&tmp_path, token)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&tmp_path, fs::Permissions::from_mode(0o600))?;
    }

    fs::rename(&tmp_path, &token_path)?;
    Ok(token_path)
}

pub fn write_port_file(uhoh_dir: &Path, port: u16) -> anyhow::Result<()> {
    let port_path = uhoh_dir.join("server.port");
    let tmp = port_path.with_extension("tmp");
    fs::write(&tmp, port.to_string())?;
    fs::rename(&tmp, &port_path)?;
    Ok(())
}

pub async fn auth_middleware(
    State(auth_config): State<AuthConfig>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path().to_string();

    // Exempt health, WebSocket, and UI HTML routes from auth.
    // The UI page must load before the JS auth flow can prompt for a token.
    if path == "/health"
        || path == "/api/v1/health"
        || path == "/ws"
        || (!path.starts_with("/api/") && path != "/mcp")
    {
        return next.run(request).await;
    }

    if path == "/mcp" {
        if !auth_config.mcp_require_auth {
            return next.run(request).await;
        }
    } else if !auth_config.require_auth {
        return next.run(request).await;
    }

    let expected = request.extensions().get::<AuthToken>().map(|t| t.0.clone());

    if let Some(expected) = expected {
        if let Some(auth_header) = headers.get("authorization") {
            if let Ok(auth_str) = auth_header.to_str() {
                if let Some(provided) = auth_str.strip_prefix("Bearer ") {
                    if token_matches(provided, &expected) {
                        return next.run(request).await;
                    }
                }
            }
        }
        return (
            StatusCode::UNAUTHORIZED,
            axum::Json(serde_json::json!({
                "error": "Bearer token required for mutating operations. Token is in your uhoh data directory (server.token)."
            })),
        )
            .into_response();
    }

    next.run(request).await
}

fn token_matches(provided: &str, expected: &str) -> bool {
    provided.as_bytes().ct_eq(expected.as_bytes()).into()
}

pub async fn host_validation_middleware(
    State(expected_port): State<u16>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    if !validate_host(&headers, expected_port) {
        return (
            StatusCode::FORBIDDEN,
            axum::Json(serde_json::json!({
                "error": "Invalid Host header"
            })),
        )
            .into_response();
    }

    next.run(request).await
}

pub fn validate_host(headers: &HeaderMap, expected_port: u16) -> bool {
    if let Some(host) = headers.get("host") {
        if let Ok(host_s) = host.to_str() {
            let allowed = [
                format!("127.0.0.1:{expected_port}"),
                format!("localhost:{expected_port}"),
                format!("[::1]:{expected_port}"),
                "127.0.0.1".to_string(),
                "localhost".to_string(),
                "[::1]".to_string(),
            ];
            return allowed.iter().any(|h| h == host_s);
        }
    }
    true
}

/// Validate the Origin header to defend against DNS rebinding attacks.
/// If an Origin header is present, it must be a loopback address.
/// Requests without an Origin header are allowed (non-browser clients).
pub fn validate_origin(headers: &HeaderMap) -> bool {
    if let Some(origin) = headers.get("origin") {
        if let Ok(origin_s) = origin.to_str() {
            // Parse the URL and check the host component exactly,
            // not with starts_with (which would accept localhost.evil.com).
            if let Ok(parsed) = url::Url::parse(origin_s) {
                if let Some(host) = parsed.host_str() {
                    return host == "127.0.0.1"
                        || host == "localhost"
                        || host == "::1"
                        || host == "[::1]";
                }
            }
            return false;
        }
        return false;
    }
    true // No Origin header (non-browser client) — allow
}
