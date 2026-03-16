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

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;

    // ── validate_host ──

    #[test]
    fn validate_host_localhost_with_port() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "localhost:22822".parse().unwrap());
        assert!(validate_host(&headers, 22822));
    }

    #[test]
    fn validate_host_127001_with_port() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "127.0.0.1:22822".parse().unwrap());
        assert!(validate_host(&headers, 22822));
    }

    #[test]
    fn validate_host_ipv6_with_port() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "[::1]:22822".parse().unwrap());
        assert!(validate_host(&headers, 22822));
    }

    #[test]
    fn validate_host_localhost_without_port() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "localhost".parse().unwrap());
        assert!(validate_host(&headers, 22822));
    }

    #[test]
    fn validate_host_rejects_external() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "evil.com:22822".parse().unwrap());
        assert!(!validate_host(&headers, 22822));
    }

    #[test]
    fn validate_host_wrong_port() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "localhost:9999".parse().unwrap());
        assert!(!validate_host(&headers, 22822));
    }

    #[test]
    fn validate_host_no_header_is_ok() {
        let headers = HeaderMap::new();
        assert!(validate_host(&headers, 22822));
    }

    // ── validate_origin ──

    #[test]
    fn validate_origin_localhost() {
        let mut headers = HeaderMap::new();
        headers.insert("origin", "http://localhost:3000".parse().unwrap());
        assert!(validate_origin(&headers));
    }

    #[test]
    fn validate_origin_127001() {
        let mut headers = HeaderMap::new();
        headers.insert("origin", "http://127.0.0.1:8080".parse().unwrap());
        assert!(validate_origin(&headers));
    }

    #[test]
    fn validate_origin_rejects_external() {
        let mut headers = HeaderMap::new();
        headers.insert("origin", "https://evil.com".parse().unwrap());
        assert!(!validate_origin(&headers));
    }

    #[test]
    fn validate_origin_no_header_is_ok() {
        let headers = HeaderMap::new();
        assert!(validate_origin(&headers));
    }

    #[test]
    fn validate_origin_invalid_url() {
        let mut headers = HeaderMap::new();
        headers.insert("origin", "not-a-url".parse().unwrap());
        assert!(!validate_origin(&headers));
    }

    // ── TransportSecurityPolicy ──

    #[test]
    fn policy_from_default_config() {
        let config = crate::config::ServerConfig::default();
        let policy = TransportSecurityPolicy::from_server_config(&config);
        assert!(policy.requires_http_auth());
        assert!(policy.requires_mcp_auth());
        assert!(policy.requires_websocket_auth());
    }

    #[test]
    fn policy_auth_disabled() {
        let config = crate::config::ServerConfig {
            require_auth: false,
            mcp_require_auth: false,
            ..crate::config::ServerConfig::default()
        };
        let policy = TransportSecurityPolicy::from_server_config(&config);
        assert!(!policy.requires_http_auth());
        assert!(!policy.requires_mcp_auth());
        assert!(!policy.requires_websocket_auth());
    }
}
