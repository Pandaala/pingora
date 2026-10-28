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

struct DefaultsOnly;

#[async_trait]
impl ProxyHttp for DefaultsOnly {
    type CTX = ();

    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        unreachable!("upstream_peer is not used by trait-default unit tests")
    }
}

#[test]
fn relay_plan_defaults_to_ordinary_replayable() {
    let io = tokio_test::io::Builder::new().build();
    let session = Session::new_h1(Box::new(io));
    assert_eq!(
        DefaultsOnly.request_relay_plan(&session, &()),
        RequestRelayPlan::ordinary()
    );
}

#[test]
fn response_head_hooks_have_fail_closed_defaults() {
    let io = tokio_test::io::Builder::new().build();
    let session = Session::new_h1(Box::new(io));
    let header = ResponseHeader::build(200, None).unwrap();
    let mut ctx = ();

    assert!(matches!(
        DefaultsOnly
            .response_head_commit_plan(&session, ResponseHeadSource::Origin, &header, &ctx,)
            .unwrap(),
        ResponseHeadCommitPlan::Immediate
    ));
    DefaultsOnly
        .response_head_will_commit(&session, &header, &mut ctx)
        .unwrap();

    match DefaultsOnly.response_head_hold_boundary(
        &session,
        ResponseHeadBoundary::Timeout,
        &mut ctx,
    ) {
        ResponseHeadBoundaryAction::Fail(error) => {
            assert_eq!(error.etype(), &InternalError);
            assert!(!error.retry());
            assert!(error.to_string().contains("timeout"));
        }
        ResponseHeadBoundaryAction::Replace(_) => {
            panic!("default Hold boundary must fail closed")
        }
    }

    DefaultsOnly.response_head_hold_outcome(
        &session,
        ResponseHeadOutcome::Failed(ResponseHeadBoundary::Timeout),
        ResponseHeadUsage::default(),
        &mut ctx,
    );
}

#[test]
fn proxy_http_remains_object_compatible() {
    fn accept_dyn(_proxy: &dyn ProxyHttp<CTX = ()>) {}

    accept_dyn(&DefaultsOnly);
}

struct LegacyBodyFilter;

#[async_trait]
impl ProxyHttp for LegacyBodyFilter {
    type CTX = bool;

    fn new_ctx(&self) -> Self::CTX {
        false
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        unreachable!("upstream_peer is not used by this body-hook unit test")
    }

    async fn request_body_filter(
        &self,
        _session: &mut Session,
        _body: &mut Option<Bytes>,
        _event: RequestBodyEvent,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        *ctx = true;
        Ok(())
    }

    async fn upstream_response_body_filter(
        &self,
        _session: &mut Session,
        _body: &mut Option<Bytes>,
        end_of_stream: bool,
        _sink: &mut ResponseBodySink,
        ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>> {
        *ctx = end_of_stream;
        Ok(None)
    }
}

#[tokio::test]
async fn action_hook_defaults_to_legacy_body_filter_and_continue() {
    let io = tokio_test::io::Builder::new().build();
    let mut session = Session::new_h1(Box::new(io));
    let mut body = Some(Bytes::from_static(b"body"));
    let mut ctx = false;
    let app = LegacyBodyFilter;

    let action = app
        .request_body_filter_action(&mut session, &mut body, RequestBodyEvent::Data, &mut ctx)
        .await
        .unwrap();

    assert!(ctx, "the legacy request_body_filter must have run");
    assert_eq!(action, RequestBodyAction::Continue);

    let trailer_action = app
        .request_trailer_filter(&mut session, &mut ctx)
        .await
        .unwrap();
    assert_eq!(trailer_action, RequestBodyAction::Continue);
}

#[tokio::test]
async fn typed_pre_trailer_terminal_defaults_to_legacy_eos() {
    let io = tokio_test::io::Builder::new().build();
    let mut session = Session::new_h1(Box::new(io));
    let mut body = None;
    let mut sink = ResponseBodySink::new();
    let mut ctx = false;

    LegacyBodyFilter
        .upstream_response_body_filter_event(
            &mut session,
            &mut body,
            UpstreamResponseBodyEvent::TerminalBeforeTrailers,
            &mut sink,
            &mut ctx,
        )
        .await
        .unwrap();

    assert!(ctx, "legacy filters must retain their terminal callback");
}
