// SPDX-License-Identifier: LGPL-3.0-or-later
//! HTTP configuration shared by calibration archive readers.

use std::{env, fs, io, time::Duration};

use ureq::tls::{PemItem, RootCerts, TlsConfig, parse_pem};

pub fn agent(timeout: Duration) -> io::Result<ureq::Agent> {
    let mut config = ureq::Agent::config_builder().timeout_global(Some(timeout));
    // Honor an explicitly configured trust bundle used by institutional HTTP
    // proxies. The standard public roots remain the default otherwise.
    if let Some(path) = env::var_os("SSL_CERT_FILE").filter(|path| !path.is_empty()) {
        let bundle = fs::read(path)?;
        let mut certificates = Vec::new();
        for item in parse_pem(&bundle) {
            if let PemItem::Certificate(certificate) = item.map_err(io::Error::other)? {
                certificates.push(certificate);
            }
        }
        if certificates.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "SSL_CERT_FILE contains no certificates",
            ));
        }
        let tls = TlsConfig::builder()
            .root_certs(RootCerts::from(certificates))
            .build();
        config = config.tls_config(tls);
    }
    Ok(config.build().into())
}
