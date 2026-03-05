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
    fs::write(&token_path, token)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&token_path)?.permissions();
        perms.set_mode(0o600);
        fs::set_permissions(&token_path, perms)?;
    }

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
    let method = request.method().clone();
    let path = request.uri().path().to_string();

    if path == "/health" || path == "/api/v1/health" || path == "/ws" {
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
                "error": "Bearer token required for mutating operations. Token is in ~/.uhoh/server.token"
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
                "127.0.0.1".to_string(),
                "localhost".to_string(),
            ];
            return allowed.iter().any(|h| h == host_s);
        }
    }
    true
}
