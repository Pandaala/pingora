// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(feature = "openssl_derived")]
mod boringssl_openssl;

#[cfg(feature = "openssl_derived")]
pub use boringssl_openssl::*;

#[cfg(feature = "rustls")]
mod rustls;

#[cfg(feature = "rustls")]
pub use rustls::*;

#[cfg(feature = "s2n")]
mod s2n;

#[cfg(feature = "s2n")]
pub use s2n::*;

#[cfg(all(test, feature = "any_tls"))]
mod tests {
    use super::TlsSettings;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn canceled_offloaded_handshake_drops_stream() {
        let cert_path = format!("{}/tests/keys/server.crt", env!("CARGO_MANIFEST_DIR"));
        let key_path = format!("{}/tests/keys/key.pem", env!("CARGO_MANIFEST_DIR"));
        let mut settings = TlsSettings::intermediate(&cert_path, &key_path).unwrap();
        settings.set_offload_threadpool(1, 1);
        let acceptor = settings.build();
        let (mut client, server) = tokio::io::duplex(1024);

        let mut handshake = Box::pin(acceptor.tls_handshake(server));
        assert!(futures::poll!(handshake.as_mut()).is_pending());
        drop(handshake);

        let mut buf = [0];
        let bytes_read = tokio::time::timeout(Duration::from_secs(1), client.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bytes_read, 0);
    }
}
