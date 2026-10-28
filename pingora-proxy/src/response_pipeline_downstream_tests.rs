// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;

#[derive(Default)]
struct Observation {
    calls: Vec<(Option<Bytes>, bool)>,
    trailer_body: Option<Bytes>,
    terminal_body: Option<Bytes>,
    fail: bool,
    delay: Option<std::time::Duration>,
}

struct Observer;

#[async_trait]
impl ProxyHttp for Observer {
    type CTX = Observation;

    fn new_ctx(&self) -> Self::CTX {
        Observation::default()
    }

    async fn upstream_peer(&self, _: &mut Session, _: &mut Self::CTX) -> Result<Box<HttpPeer>> {
        unreachable!("the pipeline test does not connect to an origin")
    }

    fn response_body_filter(
        &self,
        _: &mut Session,
        body: &mut Option<Bytes>,
        eos: bool,
        ctx: &mut Self::CTX,
    ) -> Result<Option<std::time::Duration>> {
        ctx.calls.push((body.clone(), eos));
        if eos {
            if ctx.fail {
                return Error::e_explain(InternalError, "terminal observer failure");
            }
            if body.is_none() {
                *body = ctx.terminal_body.clone();
            }
            return Ok(ctx.delay);
        }
        Ok(None)
    }

    async fn response_trailer_filter(
        &self,
        _: &mut Session,
        _: &mut http::HeaderMap,
        ctx: &mut Self::CTX,
    ) -> Result<Option<Bytes>> {
        Ok(ctx.trailer_body.clone())
    }
}

fn trailer() -> HttpTask {
    let mut headers = http::HeaderMap::new();
    headers.insert("grpc-status", http::HeaderValue::from_static("0"));
    HttpTask::Trailer(Some(Box::new(headers)))
}

#[tokio::test]
async fn downstream_terminal_observation_preserves_tasks_across_batches() {
    for tasks in [
        vec![
            HttpTask::Body(Some(Bytes::from_static(b"data")), false),
            trailer(),
            HttpTask::Done,
        ],
        vec![HttpTask::Trailer(None), HttpTask::Done],
        vec![HttpTask::Done],
        vec![HttpTask::Body(None, true), HttpTask::Done],
        vec![
            HttpTask::UpgradedBody(Some(Bytes::from_static(b"data")), true),
            HttpTask::Done,
        ],
    ] {
        let proxy = HttpProxy::new(Observer, Arc::new(ServerConf::default()));
        let mut session = request_session().await;
        let mut terminal = TerminalBodyDispatch::default();
        let mut ctx = Observation::default();
        for mut task in tasks {
            let before = format!("{task:?}");
            proxy
                .downstream_response_filter_tasks_in_order(
                    &mut session,
                    std::slice::from_mut(&mut task),
                    &mut terminal,
                    &mut ctx,
                )
                .await
                .unwrap();
            assert_eq!(
                format!("{task:?}"),
                before,
                "observation changed wire framing"
            );
        }
        assert_eq!(ctx.calls.iter().filter(|(_, eos)| *eos).count(), 1);
    }
}

#[tokio::test]
async fn downstream_trailer_body_uses_one_ordinary_terminal_callback() {
    let proxy = HttpProxy::new(Observer, Arc::new(ServerConf::default()));
    let mut session = request_session().await;
    let mut terminal = TerminalBodyDispatch::default();
    let data = Bytes::from_static(b"trailer representation");
    let mut ctx = Observation {
        trailer_body: Some(data.clone()),
        ..Observation::default()
    };
    let mut tasks = [trailer(), HttpTask::Done];
    proxy
        .downstream_response_filter_tasks_in_order(
            &mut session,
            &mut tasks,
            &mut terminal,
            &mut ctx,
        )
        .await
        .unwrap();
    assert_eq!(ctx.calls, vec![(Some(data.clone()), true)]);
    assert!(matches!(&tasks[0], HttpTask::Body(Some(body), true) if body == &data));
    assert!(matches!(tasks[1], HttpTask::Done));
}

#[tokio::test]
async fn downstream_failed_response_has_no_successful_terminal_observation() {
    let proxy = HttpProxy::new(Observer, Arc::new(ServerConf::default()));
    let mut session = request_session().await;
    let mut terminal = TerminalBodyDispatch::default();
    let mut ctx = Observation::default();
    for mut task in [
        HttpTask::Failed(Error::explain(InternalError, "source failed")),
        HttpTask::Done,
    ] {
        proxy
            .downstream_response_filter_tasks_in_order(
                &mut session,
                std::slice::from_mut(&mut task),
                &mut terminal,
                &mut ctx,
            )
            .await
            .unwrap();
    }
    assert!(ctx.calls.is_empty());
}

#[tokio::test]
async fn downstream_empty_terminal_rejects_new_bytes_and_propagates_hook_errors() {
    for fail in [false, true] {
        for terminal_task in [trailer(), HttpTask::Done] {
            let proxy = HttpProxy::new(Observer, Arc::new(ServerConf::default()));
            let mut session = request_session().await;
            let mut terminal = TerminalBodyDispatch::default();
            let mut ctx = Observation {
                terminal_body: Some(Bytes::from_static(b"invalid extra bytes")),
                fail,
                ..Observation::default()
            };
            let mut tasks = [terminal_task];
            let before = format!("{tasks:?}");
            let error = proxy
                .downstream_response_filter_tasks_in_order(
                    &mut session,
                    &mut tasks,
                    &mut terminal,
                    &mut ctx,
                )
                .await
                .unwrap_err();
            assert!(error.to_string().contains(if fail {
                "terminal observer failure"
            } else {
                "cannot produce bytes"
            }));
            assert_eq!(format!("{tasks:?}"), before);
        }
    }
}

#[tokio::test]
async fn downstream_empty_terminal_accepts_empty_output_and_honors_delay() {
    let proxy = HttpProxy::new(Observer, Arc::new(ServerConf::default()));
    let mut session = request_session().await;
    let mut terminal = TerminalBodyDispatch::default();
    let delay = std::time::Duration::from_millis(20);
    let mut ctx = Observation {
        terminal_body: Some(Bytes::new()),
        delay: Some(delay),
        ..Observation::default()
    };
    let mut tasks = [trailer(), HttpTask::Done];
    let started = tokio::time::Instant::now();
    proxy
        .downstream_response_filter_tasks_in_order(
            &mut session,
            &mut tasks,
            &mut terminal,
            &mut ctx,
        )
        .await
        .unwrap();
    assert!(started.elapsed() >= delay);
    assert_eq!(ctx.calls, vec![(None, true)]);
    assert!(matches!(tasks[0], HttpTask::Trailer(Some(_))));
}
