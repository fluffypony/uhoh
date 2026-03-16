use axum::{
    extract::Request,
    extract::State,
    http::{HeaderMap, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use rand::Rng;
use std::fs;
use std::path::{Path, PathBuf};
use subtle::ConstantTimeEq;

use crate::server::transport_security::TransportSecurityPolicy;

#[derive(Clone)]
#[non_exhaustive]
pub struct AuthToken(pub String);

pub fn generate_token() -> String {
    let mut rng = rand::thread_rng();
    let bytes: [u8; 32] = rng.gen();
    hex::encode(bytes)
}

pub(crate) fn write_token_file(uhoh_dir: &Path, token: &str) -> anyhow::Result<PathBuf> {
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

pub(crate) fn write_port_file(uhoh_dir: &Path, port: u16) -> anyhow::Result<()> {
    let port_path = uhoh_dir.join("server.port");
    let tmp = port_path.with_extension("tmp");
    fs::write(&tmp, port.to_string())?;
    fs::rename(&tmp, &port_path)?;
    Ok(())
}

pub async fn auth_middleware(
    State(auth_policy): State<TransportSecurityPolicy>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path().to_string();
    let method = request.method().clone();

    // Exempt health, WebSocket, and UI HTML routes from auth.
    // The UI page must load before the JS auth flow can prompt for a token.
    if path == "/health"
        || path == "/api/v1/health"
        || (!path.starts_with("/api/") && path != "/mcp" && path != "/ws")
    {
        return next.run(request).await;
    }

    if path == "/mcp" {
        if !auth_policy.requires_mcp_auth() {
            return next.run(request).await;
        }
    } else if path == "/ws"
        || !auth_policy.requires_http_auth()
        || matches!(method, Method::GET | Method::HEAD)
    {
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
                "error": "Bearer token required for write operations. Token is in your uhoh data directory (server.token)."
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
    State(transport_policy): State<TransportSecurityPolicy>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    if !transport_policy.validate_host(&headers) {
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

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_token_length() {
        let token = generate_token();
        assert_eq!(token.len(), 64, "Token should be 64 hex chars (32 bytes)");
    }

    #[test]
    fn generate_token_is_hex() {
        let token = generate_token();
        assert!(
            token.chars().all(|c| c.is_ascii_hexdigit()),
            "Token should be all hex characters"
        );
    }

    #[test]
    fn generate_token_unique() {
        let t1 = generate_token();
        let t2 = generate_token();
        assert_ne!(t1, t2, "Two generated tokens should differ");
    }

    #[test]
    fn token_matches_correct() {
        assert!(token_matches("secret123", "secret123"));
    }

    #[test]
    fn token_matches_wrong() {
        assert!(!token_matches("wrong", "secret123"));
    }

    #[test]
    fn token_matches_empty() {
        assert!(token_matches("", ""));
        assert!(!token_matches("", "notempty"));
        assert!(!token_matches("notempty", ""));
    }

    #[test]
    fn write_token_file_creates_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write_token_file(tmp.path(), "test-token").unwrap();
        assert!(path.exists());
        let content = fs::read_to_string(&path).unwrap();
        assert_eq!(content, "test-token");
    }

    #[cfg(unix)] // rename-replace is not atomic on Windows
    #[test]
    fn write_token_file_overwrites() {
        let tmp = tempfile::tempdir().unwrap();
        write_token_file(tmp.path(), "first").unwrap();
        write_token_file(tmp.path(), "second").unwrap();
        let content = fs::read_to_string(tmp.path().join("server.token")).unwrap();
        assert_eq!(content, "second");
    }

    #[test]
    fn write_port_file_creates_file() {
        let tmp = tempfile::tempdir().unwrap();
        write_port_file(tmp.path(), 22822).unwrap();
        let content = fs::read_to_string(tmp.path().join("server.port")).unwrap();
        assert_eq!(content, "22822");
    }

    #[cfg(unix)] // rename-replace is not atomic on Windows
    #[test]
    fn write_port_file_overwrites() {
        let tmp = tempfile::tempdir().unwrap();
        write_port_file(tmp.path(), 8080).unwrap();
        write_port_file(tmp.path(), 9090).unwrap();
        let content = fs::read_to_string(tmp.path().join("server.port")).unwrap();
        assert_eq!(content, "9090");
    }
}
