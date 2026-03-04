use axum::{
    extract::Request,
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use rand::Rng;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone)]
pub struct AuthToken(pub String);

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
    fs::write(uhoh_dir.join("server.port"), port.to_string())?;
    Ok(())
}

pub async fn auth_middleware(headers: HeaderMap, request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();

    if method == axum::http::Method::GET || path == "/health" {
        return next.run(request).await;
    }

    let expected = request
        .extensions()
        .get::<AuthToken>()
        .map(|t| t.0.clone());

    if let Some(expected) = expected {
        if let Some(auth_header) = headers.get("authorization") {
            if let Ok(auth_str) = auth_header.to_str() {
                if let Some(provided) = auth_str.strip_prefix("Bearer ") {
                    if provided == expected {
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

pub fn validate_host(headers: &HeaderMap, expected_port: u16) -> bool {
    if let Some(host) = headers.get("host") {
        if let Ok(host_s) = host.to_str() {
            let allowed = [
                format!("127.0.0.1:{}", expected_port),
                format!("localhost:{}", expected_port),
                "127.0.0.1".to_string(),
                "localhost".to_string(),
            ];
            return allowed.iter().any(|h| h == host_s);
        }
    }
    true
}
