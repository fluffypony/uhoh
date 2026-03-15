use anyhow::{Context, Result};
use rustls::RootCertStore;

use tokio_postgres::NoTls;
use tokio_postgres_rustls::MakeRustlsConnect;

use super::credentials;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum PostgresConnectionShape {
    Url,
    Keywords,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ResolvedPostgresConnection {
    connect_dsn: String,
    scrubbed_ref: String,
    shape: PostgresConnectionShape,
}

impl ResolvedPostgresConnection {
    pub(crate) fn resolve(connection_ref: &str) -> Result<Self> {
        let shape = parse_connection_shape(connection_ref);
        let creds = credentials::resolve_postgres_credentials(connection_ref)?;
        let connect_dsn = build_connect_dsn(connection_ref, &creds, shape)?;
        Ok(Self {
            scrubbed_ref: credentials::scrub_dsn(connection_ref),
            connect_dsn,
            shape,
        })
    }

    pub(crate) fn connect_dsn(&self) -> &str {
        &self.connect_dsn
    }

    pub(crate) fn scrubbed_ref(&self) -> &str {
        &self.scrubbed_ref
    }
}

pub(crate) async fn connect_postgres_client(
    connection: &ResolvedPostgresConnection,
) -> Result<tokio_postgres::Client> {
    let scrubbed_ref = connection.scrubbed_ref().to_string();
    if connection_requires_tls(connection.connect_dsn()) {
        let sslmode = extract_sslmode(connection.connect_dsn()).unwrap_or("require");
        let tls = native_rustls_connector_for_sslmode(sslmode)?;
        let (client, connection_task) = tokio_postgres::connect(connection.connect_dsn(), tls)
            .await
            .map_err(|err| anyhow::anyhow!(credentials::scrub_error_message(&err.to_string())))?;
        spawn_connection_task(scrubbed_ref, connection_task);
        return Ok(client);
    }

    let (client, connection_task) = tokio_postgres::connect(connection.connect_dsn(), NoTls)
        .await
        .map_err(|err| anyhow::anyhow!(credentials::scrub_error_message(&err.to_string())))?;
    spawn_connection_task(scrubbed_ref, connection_task);
    Ok(client)
}

fn parse_connection_shape(connection_ref: &str) -> PostgresConnectionShape {
    if connection_ref.starts_with("postgres://") || connection_ref.starts_with("postgresql://") {
        PostgresConnectionShape::Url
    } else {
        PostgresConnectionShape::Keywords
    }
}

fn build_connect_dsn(
    connection_ref: &str,
    creds: &credentials::CredentialMaterial,
    shape: PostgresConnectionShape,
) -> Result<String> {
    match shape {
        PostgresConnectionShape::Url => {
            let mut url = url::Url::parse(connection_ref).with_context(|| {
                format!("Invalid Postgres connection reference: {connection_ref}")
            })?;

            if let Some(ref user) = creds.username {
                let _ = url.set_username(user);
            }
            if let Some(ref pw) = creds.password {
                let _ = url.set_password(Some(pw));
            }

            Ok(url.to_string())
        }
        PostgresConnectionShape::Keywords => {
            let mut parts: Vec<String> = connection_ref
                .split_whitespace()
                .map(std::string::ToString::to_string)
                .collect();
            let has_user = parts.iter().any(|part| {
                part.split_once('=')
                    .is_some_and(|(key, _)| key.eq_ignore_ascii_case("user"))
            });
            let has_password = parts.iter().any(|part| {
                part.split_once('=')
                    .is_some_and(|(key, _)| key.eq_ignore_ascii_case("password"))
            });
            if let Some(ref user) = creds.username {
                if !has_user {
                    parts.push(format!("user={user}"));
                }
            }
            if let Some(ref pw) = creds.password {
                if !has_password {
                    parts.push(format!("password={pw}"));
                }
            }
            Ok(parts.join(" "))
        }
    }
}

fn spawn_connection_task<F>(scrubbed_ref: String, connection: F)
where
    F: std::future::Future<Output = std::result::Result<(), tokio_postgres::Error>>
        + Send
        + 'static,
{
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(
                "postgres connection task exited for {}: {}",
                scrubbed_ref,
                credentials::scrub_error_message(&err.to_string())
            );
        }
    });
}

/// Certificate verifier that accepts any server certificate (for sslmode=require).
#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

fn native_rustls_connector_for_sslmode(sslmode: &str) -> Result<MakeRustlsConnect> {
    let config = match sslmode {
        "verify-ca" | "verify-full" => {
            let mut roots = RootCertStore::empty();
            let native = rustls_native_certs::load_native_certs();
            if !native.errors.is_empty() {
                let first = &native.errors[0];
                tracing::warn!("native certificate load issue: {first}");
            }
            for cert in native.certs {
                if let Err(err) = roots.add(cert) {
                    tracing::warn!("skipping invalid native certificate: {err}");
                }
            }
            if roots.is_empty() {
                anyhow::bail!("No trusted root certificates available for Postgres TLS connection")
            }
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth()
        }
        _ => rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(std::sync::Arc::new(NoVerifier))
            .with_no_client_auth(),
    };
    Ok(MakeRustlsConnect::new(config))
}

fn extract_sslmode(connection_ref: &str) -> Option<&'static str> {
    if let Ok(url) = url::Url::parse(connection_ref) {
        for (key, value) in url.query_pairs() {
            if key.eq_ignore_ascii_case("sslmode") {
                let mode = value.to_ascii_lowercase();
                return Some(match mode.as_str() {
                    "verify-ca" => "verify-ca",
                    "verify-full" => "verify-full",
                    _ => "require",
                });
            }
        }
    }
    for part in connection_ref.split_whitespace() {
        if let Some((key, value)) = part.split_once('=') {
            if key.eq_ignore_ascii_case("sslmode") {
                let mode = value
                    .trim_matches('"')
                    .trim_matches('\'')
                    .to_ascii_lowercase();
                return Some(match mode.as_str() {
                    "verify-ca" => "verify-ca",
                    "verify-full" => "verify-full",
                    _ => "require",
                });
            }
        }
    }
    None
}

fn connection_requires_tls(connection_ref: &str) -> bool {
    if let Ok(url) = url::Url::parse(connection_ref) {
        if matches!(url.scheme(), "postgres" | "postgresql") {
            for (key, value) in url.query_pairs() {
                if key.eq_ignore_ascii_case("sslmode") {
                    let mode = value.to_ascii_lowercase();
                    if matches!(mode.as_str(), "require" | "verify-ca" | "verify-full") {
                        return true;
                    }
                }
            }
        }
    }

    for part in connection_ref.split_whitespace() {
        if let Some((key, value)) = part.split_once('=') {
            if key.eq_ignore_ascii_case("sslmode") {
                let mode = value
                    .trim_matches('"')
                    .trim_matches('\'')
                    .to_ascii_lowercase();
                if matches!(mode.as_str(), "require" | "verify-ca" | "verify-full") {
                    return true;
                }
            }
        }
    }

    false
}
