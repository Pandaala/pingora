// Copyright 2026 Cloudflare, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use pingora_core::modules::http::{HttpModule, HttpModuleBuilder, HttpModules, Module};
use std::any::Any;
use tokio::io::AsyncReadExt;

#[tokio::test]
async fn released_trailer_hook_failure_does_not_write_the_prepared_head() {
    for protocol in [ResponseProtocol::H1, ResponseProtocol::H2] {
        let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
        let (mut client, server) = tokio::io::duplex(8192);
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
            .await
            .unwrap();
        let mut session = Session::new_h1(Box::new(server) as pingora_core::protocols::Stream);
        session.read_request().await.unwrap();
        let mut ctx = HoldReleaseCtx {
            release_on_body_call: Some(1),
            fail_downstream_trailer: true,
            ..Default::default()
        };
        let mut state = ResponsePipelineState::default();
        let mut cache = ServeFromCache::new();
        let mut out = Vec::new();
        proxy
            .response_task_pipeline(
                protocol,
                &mut session,
                HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false),
                &mut ctx,
                &mut cache,
                false,
                &mut state,
                &mut out,
            )
            .await
            .unwrap();
        assert!(out.is_empty());
        let mut trailers = http::HeaderMap::new();
        trailers.insert("x-finish", http::HeaderValue::from_static("yes"));
        let error = proxy
            .response_task_pipeline(
                protocol,
                &mut session,
                HttpTask::Trailer(Some(Box::new(trailers))),
                &mut ctx,
                &mut cache,
                false,
                &mut state,
                &mut out,
            )
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("downstream trailer failure marker"));
        assert_eq!(ctx.trailer_capabilities, vec![true]);
        assert_eq!(ctx.will_commit_calls.load(Ordering::Relaxed), 1);
        assert_eq!(session.prepared_response_headers, 1);
        assert!(session.as_downstream().response_written().is_none());
        drop(session);
        let mut wire = Vec::new();
        client.read_to_end(&mut wire).await.unwrap();
        assert!(wire.is_empty());
    }
}

struct FramingModule {
    calls: Arc<AtomicUsize>,
    chunked: Option<bool>,
}

#[async_trait]
impl HttpModule for FramingModule {
    async fn response_header_filter(&mut self, head: &mut ResponseHeader, _: bool) -> Result<()> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        match self.chunked {
            Some(true) => {
                head.remove_header(&http::header::CONTENT_LENGTH);
                head.insert_header(http::header::TRANSFER_ENCODING, "chunked")?;
            }
            Some(false) => {
                head.remove_header(&http::header::TRANSFER_ENCODING);
                head.insert_header(http::header::CONTENT_LENGTH, "4")?;
            }
            None => {}
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl HttpModuleBuilder for FramingModule {
    fn init(&self) -> Module {
        Box::new(Self {
            calls: self.calls.clone(),
            chunked: self.chunked,
        })
    }
}

// Exercise the actual H1 writer as well as the application's pre-write query.
// Header modules deliberately change framing in both directions.
#[tokio::test]
async fn released_trailers_observe_selected_framing_and_reach_the_wire() {
    for protocol in [ResponseProtocol::H1, ResponseProtocol::H2] {
        for (http10, content_length, module_chunked, expected) in [
            (false, false, None, true),
            (true, false, None, false),
            (false, true, None, false),
            (false, true, Some(true), true),
            (false, false, Some(false), false),
        ] {
            // None: Immediate; Some(1): release on data; Some(2): release on trailer.
            for release_call in [None, Some(1), Some(2)] {
                let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
                let (mut client, server) = tokio::io::duplex(8192);
                let request = if http10 {
                    b"GET / HTTP/1.0\r\nHost: example.com\r\n\r\n".as_slice()
                } else {
                    b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n".as_slice()
                };
                client.write_all(request).await.unwrap();
                let mut session =
                    Session::new_h1(Box::new(server) as pingora_core::protocols::Stream);
                session.read_request().await.unwrap();
                let calls = Arc::new(AtomicUsize::new(0));
                let mut modules = HttpModules::new();
                modules.add_module(Box::new(FramingModule {
                    calls: calls.clone(),
                    chunked: module_chunked,
                }));
                session.downstream_modules_ctx = modules.build_ctx();
                let mut ctx = HoldReleaseCtx {
                    immediate: release_call.is_none(),
                    release_on_body_call: release_call,
                    ..Default::default()
                };
                let mut state = ResponsePipelineState::default();
                let mut cache = ServeFromCache::new();
                let mut out = Vec::new();
                let mut head = ResponseHeader::build(200, None).unwrap();
                if content_length {
                    head.insert_header(http::header::CONTENT_LENGTH, "4")
                        .unwrap();
                }
                let mut trailers = http::HeaderMap::new();
                trailers.insert("x-finish", http::HeaderValue::from_static("yes"));
                for (index, task) in [
                    HttpTask::Header(Box::new(head), false),
                    HttpTask::Body(Some(Bytes::from_static(b"body")), false),
                    HttpTask::Trailer(Some(Box::new(trailers))),
                    HttpTask::Done,
                ]
                .into_iter()
                .enumerate()
                {
                    proxy
                        .response_task_pipeline(
                            protocol,
                            &mut session,
                            task,
                            &mut ctx,
                            &mut cache,
                            false,
                            &mut state,
                            &mut out,
                        )
                        .await
                        .unwrap();
                    if release_call.is_some_and(|call| index < call) {
                        assert!(out.is_empty());
                        assert_eq!(calls.load(Ordering::Relaxed), 0);
                        assert_eq!(session.prepared_response_headers, 0);
                        assert_eq!(ctx.will_commit_calls.load(Ordering::Relaxed), 0);
                    }
                }
                assert_eq!(ctx.trailer_capabilities, vec![expected], "{protocol:?}, HTTP/1.0={http10}, CL={content_length}, module={module_chunked:?}, release={release_call:?}");
                assert_eq!(calls.load(Ordering::Relaxed), 1);
                assert_eq!(ctx.will_commit_calls.load(Ordering::Relaxed), 1);
                session.write_response_tasks(out).await.unwrap();
                assert_eq!(calls.load(Ordering::Relaxed), 1);
                assert_eq!(session.prepared_response_headers, 0);
                drop(session);
                let mut wire = Vec::new();
                client.read_to_end(&mut wire).await.unwrap();
                let wire = String::from_utf8(wire).unwrap();
                assert!(wire.contains("body"), "{wire}");
                assert_eq!(
                    wire.matches("x-finish: yes\r\n").count(),
                    usize::from(expected),
                    "{wire}"
                );
            }
        }
    }
}
