use axum::http::HeaderMap;

#[derive(Clone, Copy)]
pub struct TransportSecurityPolicy {
    expected_port: u16,
    require_http_auth: bool,
    require_mcp_auth: bool,
}

impl TransportSecurityPolicy {
    pub fn from_server_config(config: &crate::config::ServerConfig) -> Self {
        Self {
            expected_port: config.port,
            require_http_auth: config.require_auth,
            require_mcp_auth: config.mcp_require_auth,
        }
    }

    pub fn requires_http_auth(self) -> bool {
        self.require_http_auth
    }

    pub fn requires_mcp_auth(self) -> bool {
        self.require_mcp_auth
    }

    pub fn requires_websocket_auth(self) -> bool {
        self.require_http_auth
    }

    pub fn validate_host(self, headers: &HeaderMap) -> bool {
        validate_host(headers, self.expected_port)
    }

    pub fn validate_origin(self, headers: &HeaderMap) -> bool {
        validate_origin(headers)
    }
}

pub fn validate_host(headers: &HeaderMap, expected_port: u16) -> bool {
    if let Some(host) = headers.get("host") {
        let Ok(host_s) = host.to_str() else {
            return false;
        };
        let allowed = [
            format!("127.0.0.1:{expected_port}"),
            format!("localhost:{expected_port}"),
            format!("[::1]:{expected_port}"),
            "127.0.0.1".to_string(),
            "localhost".to_string(),
            "[::1]".to_string(),
        ];
        return allowed.iter().any(|candidate| candidate == host_s);
    }
    true
}

pub fn validate_origin(headers: &HeaderMap) -> bool {
    if let Some(origin) = headers.get("origin") {
        let Ok(origin_s) = origin.to_str() else {
            return false;
        };
        let Ok(parsed) = url::Url::parse(origin_s) else {
            return false;
        };
        let Some(host) = parsed.host_str() else {
            return false;
        };
        return matches!(host, "127.0.0.1" | "localhost" | "::1");
    }
    true
}
