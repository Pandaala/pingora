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

use super::*;
use pingora_cache::{
    key::HashBinary,
    CacheKey, CacheMeta, ForcedFreshness, HitHandler,
    RespCacheable::{self, *},
};
use proxy_cache::range_filter::{self};
use std::time::Duration;

/// The action selected by an application request-body hook.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestBodyAction {
    /// Continue proxying the current request-body event.
    Continue,
    /// Stop proxying this request after the application completed or aborted
    /// the downstream response.
    ///
    /// H1 truncation caveat: terminating while large unread request bytes are
    /// still in flight closes the downstream connection with data pending, so
    /// the OS may send a RST. A client that is not reading the response
    /// concurrently with its upload may then never see the response the
    /// application wrote. This is a known limitation and matches the existing
    /// `close_on_response_before_downstream_finish` semantics.
    ///
    /// Upstream cost caveat: these hooks run only after the upstream request
    /// headers have been written, so a terminate always costs the upstream
    /// attempt that was already started. On an H1 upstream it also costs the
    /// CONNECTION: the request body was cut short, so the connection is no
    /// longer in a well-defined state and cannot be returned to the pool. (On
    /// an H2 upstream only the stream is lost, via RST_STREAM.) An application
    /// that can reach its decision from the request headers alone should
    /// reject in [`ProxyHttp::request_filter`] instead, which costs nothing
    /// upstream.
    Terminate,
}

/// How the proxy should frame the request body sent to an upstream.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum UpstreamRequestBodyDisposition {
    /// Preserve Pingora's ordinary request-body framing behavior.
    #[default]
    Ordinary,
    /// The application guarantees that no upstream request body will follow.
    ///
    /// The proxy acts on the guarantee irreversibly, before a single body byte
    /// is read: an H2 upstream request is closed with END_STREAM on its HEADERS
    /// frame (or on an empty DATA frame), and an H1 upstream request loses both
    /// `Content-Length` and `Transfer-Encoding`.
    ///
    /// Selecting this on a request whose downstream body then DOES carry bytes
    /// is a contract violation and fails the request with an
    /// [`InternalError`](pingora_error::ErrorType::InternalError), which the
    /// default [`ProxyHttp::fail_to_proxy`] turns into a 500. The proxy fails
    /// closed rather than forward the request without its body: the upstream
    /// would otherwise act on a request whose client-supplied body was silently
    /// removed while the client is told it succeeded, and no proxy can judge
    /// that substitution safe.
    ///
    /// A request with no body at all is unaffected: it is coerced back to
    /// [`Self::Ordinary`] before the pump runs (see
    /// [`ProxyHttp::upstream_request_body_disposition`]) and proxies normally.
    /// An application that is not certain whether a body will arrive must
    /// select [`Self::Ordinary`] (or [`Self::Streamed`]) instead; an
    /// application that wants to DROP a body it knows about must remove it in
    /// [`ProxyHttp::request_body_filter_action`], which runs before this check.
    Bodyless,
    /// The body is streamed and its final length is not known when headers
    /// are sent.
    Streamed,
}

/// The interface to control the HTTP proxy
///
/// The methods in [ProxyHttp] are filters/callbacks which will be performed on all requests at their
/// particular stage (if applicable).
///
/// If any of the filters returns [Result::Err], the request will fail, and the error will be logged.
#[cfg_attr(not(doc_async_trait), async_trait)]
pub trait ProxyHttp {
    /// The per request object to share state across the different filters
    type CTX;

    /// Define how the `ctx` should be created.
    fn new_ctx(&self) -> Self::CTX;

    /// Define where the proxy should send the request to.
    ///
    /// The returned [HttpPeer] contains the information regarding where and how this request should
    /// be forwarded to.
    async fn upstream_peer(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>>;

    /// Set up downstream modules.
    ///
    /// In this phase, users can add or configure [HttpModules] before the server starts up.
    ///
    /// In the default implementation of this method, [ResponseCompressionBuilder] is added
    /// and disabled.
    fn init_downstream_modules(&self, modules: &mut HttpModules) {
        // Add disabled downstream compression module by default
        modules.add_module(ResponseCompressionBuilder::enable(0));
    }

    /// Handle the incoming request.
    ///
    /// In this phase, users can parse, validate, rate limit, perform access control and/or
    /// return a response for this request.
    ///
    /// If the user already sent a response to this request, an `Ok(true)` should be returned so that
    /// the proxy would exit. The proxy continues to the next phases when `Ok(false)` is returned.
    ///
    /// By default this filter does nothing and returns `Ok(false)`.
    async fn request_filter(&self, _session: &mut Session, _ctx: &mut Self::CTX) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        Ok(false)
    }

    /// Handle the incoming request before any downstream module is executed.
    ///
    /// This function is similar to [Self::request_filter()] but executes before any other logic,
    /// including downstream module logic. The main purpose of this function is to provide finer
    /// grained control of the behavior of the modules.
    ///
    /// Note that because this function is executed before any module that might provide access
    /// control or rate limiting, logic should stay in request_filter() if it can in order to be
    /// protected by said modules.
    async fn early_request_filter(&self, _session: &mut Session, _ctx: &mut Self::CTX) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        Ok(())
    }

    /// Returns whether this session is allowed to spawn subrequests.
    ///
    /// This function is checked after [Self::early_request_filter] to allow that filter to configure
    /// this if required. This will also run for subrequests themselves, which may allowed to spawn
    /// their own subrequests.
    ///
    /// Note that this doesn't prevent subrequests from being spawned based on the session by proxy
    /// core functionality, e.g. background cache revalidation requires spawning subrequests.
    fn allow_spawning_subrequest(&self, _session: &Session, _ctx: &Self::CTX) -> bool
    where
        Self::CTX: Send + Sync,
    {
        false
    }

    /// Handle the incoming request body.
    ///
    /// This function will be called every time a piece of request body is received. The `body` is
    /// **not the entire request body**.
    ///
    /// The async nature of this function allows to throttle the upload speed and/or executing
    /// heavy computation logic such as WAF rules on offloaded threads without blocking the threads
    /// who process the requests themselves.
    async fn request_body_filter(
        &self,
        _session: &mut Session,
        _body: &mut Option<Bytes>,
        _end_of_stream: bool,
        _ctx: &mut Self::CTX,
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        Ok(())
    }

    /// Handle one request-body event and decide whether proxying should
    /// continue.
    ///
    /// Before returning [`RequestBodyAction::Terminate`] the application must
    /// have finished the downstream response itself — either by writing a
    /// local reply, or by observing an already-committed response and
    /// abandoning. Pingora never writes a response on the terminate path; a
    /// terminate with nothing written leaves the client with a bare connection
    /// close, and Pingora logs a warning when it detects that.
    ///
    /// On custom-connector sessions terminate is unsupported: the pump fails
    /// closed with an [`InternalError`](pingora_error::ErrorType::InternalError)
    /// instead.
    ///
    /// The default preserves source compatibility by invoking
    /// [`Self::request_body_filter`] and returning
    /// [`RequestBodyAction::Continue`].
    async fn request_body_filter_action(
        &self,
        session: &mut Session,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> Result<RequestBodyAction>
    where
        Self::CTX: Send + Sync,
    {
        self.request_body_filter(session, body, end_of_stream, ctx)
            .await?;
        Ok(RequestBodyAction::Continue)
    }

    /// Handle the presence of actual downstream request trailer fields.
    ///
    /// The hook runs after the trailer-presence fact is established and
    /// before a trailer-free synthetic request-body EOF could be delivered.
    /// Pingora does not expose or forward the trailer fields.
    ///
    /// It fires AT MOST ONCE per downstream request, on the attempt that
    /// observes the transport EOF. Retry attempts replay the same EOF and do
    /// not re-fire it.
    ///
    /// It never fires on custom-connector sessions: the trailer-presence fact
    /// is `None` there.
    ///
    /// It also fires only when the PUMP owns the request-body read, and only
    /// when the pump actually observes the transport EOF. Two shapes skip it:
    /// - The application consumed the downstream body itself before proxying
    ///   started, without registering a replay buffer, so the pump has nothing
    ///   to read and never sees the EOF. An inspection policy that depends on
    ///   this hook must therefore not pre-read the request body.
    /// - The pump abandoned the downstream read as futile: the upstream
    ///   response completed while the client never ended a request body it had
    ///   declared empty (`Content-Length: 0` without END_STREAM on H2). The
    ///   pump then synthesizes the single
    ///   [`Self::request_body_filter_action`] end-of-stream event, but the
    ///   trailer-presence fact was never established, so a request that would
    ///   have sent TRAILERS later yields no trailer event.
    ///
    /// The [`RequestBodyAction::Terminate`] contract is the same as for
    /// [`Self::request_body_filter_action`]: the application must have
    /// finished the downstream response before returning it.
    async fn request_trailer_filter(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<RequestBodyAction>
    where
        Self::CTX: Send + Sync,
    {
        Ok(RequestBodyAction::Continue)
    }

    /// Select the upstream request-body framing contract.
    ///
    /// Queried once per upstream attempt, before the upstream request
    /// headers are written. Sync on purpose: `ProxyHttp` is an
    /// `#[async_trait]`, so async methods are boxed per call while sync
    /// defaults monomorphize and inline away.
    ///
    /// Several request shapes override the returned value. On each of them a
    /// non-`Ordinary` disposition is logged at debug level and coerced back to
    /// [`UpstreamRequestBodyDisposition::Ordinary`]:
    /// - An upgrade request (`Upgrade:` header) or a CONNECT request: the
    ///   framing is fixed by the tunnel protocol. The check looks at the union
    ///   of the downstream request and the (already filtered) upstream
    ///   request, so synthesizing a tunnel in
    ///   [`Self::upstream_request_filter`] is covered too.
    /// - A request with no body at all: re-framing it (e.g. as
    ///   `Transfer-Encoding: chunked`) would put a body terminator on a pooled
    ///   upstream connection that the origin may ignore, which desynchronises
    ///   every later request on it.
    /// - An upstream request still versioned below HTTP/1.1, which has no
    ///   chunked framing.
    ///
    /// On custom-connector sessions the connector owns framing, so a
    /// non-`Ordinary` disposition fails the request closed with an
    /// [`InternalError`](pingora_error::ErrorType::InternalError). Returning
    /// [`UpstreamRequestBodyDisposition::Bodyless`] for a request that then
    /// does carry a downstream body fails closed the same way; see that
    /// variant's documentation.
    fn upstream_request_body_disposition(
        &self,
        _session: &Session,
        _ctx: &Self::CTX,
    ) -> UpstreamRequestBodyDisposition {
        UpstreamRequestBodyDisposition::Ordinary
    }

    /// Whether this request may make another upstream proxy attempt.
    ///
    /// Queried live at every retry decision point and never cached, so an
    /// application may flip from `true` to `false` mid-request. The
    /// application must keep the predicate monotonic (once `false`, never
    /// `true` again). Returning `false` also suppresses native request-body
    /// retry buffering.
    ///
    /// Sampling points, in order:
    /// 1. before the upstream request body is pumped, to decide whether to
    ///    enable native retry buffering;
    /// 2. after [`Self::error_while_proxy`] returns (and after
    ///    [`Self::fail_to_connect`] on the connect path), so a predicate the
    ///    application flips from inside those hooks is honored for that very
    ///    error;
    /// 3. once more in the retry loop, alongside the error's own retry
    ///    classification.
    ///
    /// Interaction with `Session::retry_buffer_truncated()`: returning `false`
    /// skips the retry-buffer allocation entirely, so nothing is ever
    /// buffered and nothing can be reported as truncated —
    /// `retry_buffer_truncated()` stays `false` ("nothing was truncated")
    /// even though the request body is not replayable at all. An application
    /// that overrides [`Self::error_while_proxy`] and keys its retry decision
    /// on that flag must therefore consult this predicate as well; the flag
    /// alone cannot distinguish "fully buffered" from "never buffered".
    fn request_retry_allowed(&self, _session: &Session, _ctx: &Self::CTX) -> bool {
        true
    }

    /// This filter decides if the request is cacheable and what cache backend to use
    ///
    /// The caller can interact with `Session.cache` to enable caching.
    ///
    /// By default this filter does nothing which effectively disables caching.
    // Ideally only session.cache should be modified, TODO: reflect that in this interface
    fn request_cache_filter(&self, _session: &mut Session, _ctx: &mut Self::CTX) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        Ok(())
    }

    /// This callback generates the cache key.
    ///
    /// This callback is called only when cache is enabled for this request.
    ///
    /// There is no sensible default cache key for all proxy applications. The
    /// correct key depends on which request properties affect upstream responses
    /// (e.g. `Vary` headers, custom request filters that modify the origin host).
    /// Getting this wrong leads to cache poisoning.
    ///
    /// See `pingora-proxy/tests/utils/server_utils.rs` for a minimal (not
    /// production-ready) reference implementation.
    ///
    /// # Panics
    ///
    /// The default implementation panics. You **must** override this method when
    /// caching is enabled.
    fn cache_key_callback(&self, _session: &Session, _ctx: &mut Self::CTX) -> Result<CacheKey> {
        unimplemented!("cache_key_callback must be implemented when caching is enabled")
    }

    /// This callback is invoked when a cacheable response is ready to be admitted to cache.
    fn cache_miss(&self, session: &mut Session, _ctx: &mut Self::CTX) {
        session.cache.cache_miss();
    }

    /// This filter is called after a successful cache lookup and before the
    /// cache asset is ready to be used.
    ///
    /// This filter allows the user to log or force invalidate the asset, or
    /// to adjust the body reader associated with the cache hit.
    /// This also runs on stale hit assets (for which `is_fresh` is false).
    ///
    /// The value returned indicates if the force invalidation should be used,
    /// and which kind. Returning `None` indicates no forced invalidation
    async fn cache_hit_filter(
        &self,
        _session: &mut Session,
        _meta: &CacheMeta,
        _hit_handler: &mut HitHandler,
        _is_fresh: bool,
        _ctx: &mut Self::CTX,
    ) -> Result<Option<ForcedFreshness>>
    where
        Self::CTX: Send + Sync,
    {
        Ok(None)
    }

    /// Decide if a request should continue to upstream after not being served from cache.
    ///
    /// returns: Ok(true) if the request should continue, Ok(false) if a response was written by the
    /// callback and the session should be finished, or an error
    ///
    /// This filter can be used for deferring checks like rate limiting or access control to when they
    /// actually needed after cache miss.
    ///
    /// By default the session will attempt to be reused after returning Ok(false). It is the
    /// caller's responsibility to disable keepalive or drain the request body if needed.
    async fn proxy_upstream_filter(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        Ok(true)
    }

    /// Decide if the response is cacheable
    fn response_cache_filter(
        &self,
        _session: &Session,
        _resp: &ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<RespCacheable> {
        Ok(Uncacheable(NoCacheReason::Custom("default")))
    }

    /// Decide how to generate cache vary key from both request and response
    ///
    /// None means no variance is needed.
    fn cache_vary_filter(
        &self,
        _meta: &CacheMeta,
        _ctx: &mut Self::CTX,
        _req: &RequestHeader,
    ) -> Option<HashBinary> {
        // default to None for now to disable vary feature
        None
    }

    /// Decide if the incoming request's condition _fails_ against the cached response.
    ///
    /// Returning `Ok(true)` means that the response does _not_ match against the condition, and
    /// that the proxy can return `304 Not Modified` downstream.
    ///
    /// An example is a conditional GET request with `If-None-Match: "foobar"`. If the cached
    /// response contains the `ETag: "foobar"`, then the condition fails, and `304 Not Modified`
    /// should be returned. Else, the condition passes which means the full `200 OK` response must
    /// be sent.
    fn cache_not_modified_filter(
        &self,
        session: &Session,
        resp: &ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<bool> {
        Ok(
            pingora_core::protocols::http::conditional_filter::not_modified_filter(
                session.req_header(),
                resp,
            ),
        )
    }

    /// This filter is called when cache is enabled to determine what byte range to return (in both
    /// cache hit and miss cases) from the response body. It is only used when caching is enabled,
    /// otherwise the upstream is responsible for any filtering. It allows users to define the range
    /// this request is for via its return type `range_filter::RangeType`.
    ///
    /// It also allow users to modify the response header accordingly.
    ///
    /// The default implementation can handle a single-range as per [RFC7232].
    ///
    /// [RFC7232]: https://www.rfc-editor.org/rfc/rfc7232
    fn range_header_filter(
        &self,
        session: &mut Session,
        resp: &mut ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> range_filter::RangeType {
        const DEFAULT_MAX_RANGES: Option<usize> = Some(200);
        proxy_cache::range_filter::range_header_filter(
            session.req_header(),
            resp,
            DEFAULT_MAX_RANGES,
        )
    }

    /// Modify the request before it is sent to the upstream
    ///
    /// Unlike [Self::request_filter()], this filter allows to change the request headers to send
    /// to the upstream.
    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        _upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        Ok(())
    }

    /// Modify the response header from the upstream
    ///
    /// The modification is before caching, so any change here will be stored in the cache if enabled.
    ///
    /// Responses served from cache won't trigger this filter. If the cache needed revalidation,
    /// only the 304 from upstream will trigger the filter (though it will be merged into the
    /// cached header, not served directly to downstream).
    async fn upstream_response_filter(
        &self,
        _session: &mut Session,
        _upstream_response: &mut ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        Ok(())
    }

    /// Modify the response header before it is send to the downstream
    ///
    /// The modification is after caching. This filter is called for all responses including
    /// responses served from cache.
    async fn response_filter(
        &self,
        _session: &mut Session,
        _upstream_response: &mut ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        Ok(())
    }

    // custom_forwarding is called when downstream and upstream connections are successfully established.
    #[doc(hidden)]
    async fn custom_forwarding(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
        _custom_message_to_upstream: Option<mpsc::Sender<Bytes>>,
        _custom_message_to_downstream: mpsc::Sender<Bytes>,
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        Ok(())
    }

    // received a custom message from the downstream before sending it to the upstream.
    #[doc(hidden)]
    async fn downstream_custom_message_proxy_filter(
        &self,
        _session: &mut Session,
        custom_message: Bytes,
        _ctx: &mut Self::CTX,
        _final_hop: bool,
    ) -> Result<Option<Bytes>>
    where
        Self::CTX: Send + Sync,
    {
        Ok(Some(custom_message))
    }

    // received a custom message from the upstream before sending it to the downstream.
    #[doc(hidden)]
    async fn upstream_custom_message_proxy_filter(
        &self,
        _session: &mut Session,
        custom_message: Bytes,
        _ctx: &mut Self::CTX,
        _final_hop: bool,
    ) -> Result<Option<Bytes>>
    where
        Self::CTX: Send + Sync,
    {
        Ok(Some(custom_message))
    }

    /// Similar to [Self::upstream_response_filter()] but for response body
    ///
    /// This function will be called every time a piece of response body is received. The `body` is
    /// **not the entire response body**.
    ///
    /// Mutate `body` in place for the common one-in-one-out case. To emit
    /// *additional* chunks, or to end the response early, use `sink`. The
    /// sink's byte budget is per pump batch; see [`crate::ResponseBodySink`].
    async fn upstream_response_body_filter(
        &self,
        _session: &mut Session,
        _body: &mut Option<Bytes>,
        _end_of_stream: bool,
        _sink: &mut ResponseBodySink,
        _ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>>
    where
        Self::CTX: Send + Sync,
    {
        Ok(None)
    }

    /// Similar to [Self::upstream_response_filter()] but for response trailers
    fn upstream_response_trailer_filter(
        &self,
        _session: &mut Session,
        _upstream_trailers: &mut header::HeaderMap,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        Ok(())
    }

    /// Similar to [Self::response_filter()] but for response body chunks
    fn response_body_filter(
        &self,
        _session: &mut Session,
        _body: &mut Option<Bytes>,
        _end_of_stream: bool,
        _ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>>
    where
        Self::CTX: Send + Sync,
    {
        Ok(None)
    }

    /// Similar to [Self::response_filter()] but for response trailers.
    /// Note, returning an Ok(Some(Bytes)) will result in the downstream response
    /// trailers being written to the response body.
    ///
    /// TODO: make this interface more intuitive
    async fn response_trailer_filter(
        &self,
        _session: &mut Session,
        _upstream_trailers: &mut header::HeaderMap,
        _ctx: &mut Self::CTX,
    ) -> Result<Option<Bytes>>
    where
        Self::CTX: Send + Sync,
    {
        Ok(None)
    }

    /// This filter is called when the entire response is sent to the downstream successfully or
    /// there is a fatal error that terminate the request.
    ///
    /// An error log is already emitted if there is any error. This phase is used for collecting
    /// metrics and sending access logs.
    async fn logging(&self, _session: &mut Session, _e: Option<&Error>, _ctx: &mut Self::CTX)
    where
        Self::CTX: Send + Sync,
    {
    }

    /// A value of true means that the log message will be suppressed. The default value is false.
    fn suppress_error_log(&self, _session: &Session, _ctx: &Self::CTX, _error: &Error) -> bool {
        false
    }

    /// This filter is called when there is an error **after** a connection is established (or reused)
    /// to the upstream.
    fn error_while_proxy(
        &self,
        peer: &HttpPeer,
        session: &mut Session,
        e: Box<Error>,
        _ctx: &mut Self::CTX,
        client_reused: bool,
    ) -> Box<Error> {
        let mut e = e.more_context(format!("Peer: {}", peer));
        // only reused client connections where retry buffer is not truncated
        e.retry
            .decide_reuse(client_reused && !session.as_ref().retry_buffer_truncated());
        e
    }

    /// This filter is called when there is an error in the process of establishing a connection
    /// to the upstream.
    ///
    /// In this filter the user can decide whether the error is retry-able by marking the error `e`.
    ///
    /// If the error can be retried, [Self::upstream_peer()] will be called again so that the user
    /// can decide whether to send the request to the same upstream or another upstream that is possibly
    /// available.
    fn fail_to_connect(
        &self,
        _session: &mut Session,
        _peer: &HttpPeer,
        _ctx: &mut Self::CTX,
        e: Box<Error>,
    ) -> Box<Error> {
        e
    }

    /// This filter is called when the request encounters a fatal error.
    ///
    /// Users may write an error response to the downstream if the downstream is still writable.
    ///
    /// The response status code of the error response may be returned for logging purposes.
    /// Additionally, the user can return whether this session may be reused in spite of the error.
    /// Today this reuse status is only respected for errors that occur prior to upstream peer
    /// selection, and the keepalive configured on the `Session` itself still takes precedent.
    async fn fail_to_proxy(
        &self,
        session: &mut Session,
        e: &Error,
        _ctx: &mut Self::CTX,
    ) -> FailToProxy
    where
        Self::CTX: Send + Sync,
    {
        let code = match e.etype() {
            HTTPStatus(code) => *code,
            _ => {
                match e.esource() {
                    ErrorSource::Upstream => 502,
                    ErrorSource::Downstream => {
                        match e.etype() {
                            WriteError | ReadError | ConnectionClosed => {
                                /* conn already dead */
                                0
                            }
                            /* the client stopped sending its request; the
                             * request itself was never malformed, so 408 is
                             * the accurate answer rather than 400 */
                            ReadTimedout => 408,
                            _ => 400,
                        }
                    }
                    ErrorSource::Internal | ErrorSource::Unset => 500,
                }
            }
        };
        if code > 0 {
            session.respond_error(code).await.unwrap_or_else(|e| {
                error!("failed to send error response to downstream: {e}");
            });
        }

        FailToProxy {
            error_code: code,
            // default to no reuse, which is safest
            can_reuse_downstream: false,
        }
    }

    /// Decide whether should serve stale when encountering an error or during revalidation
    ///
    /// An implementation should follow
    /// <https://datatracker.ietf.org/doc/html/rfc9111#section-4.2.4>
    /// <https://www.rfc-editor.org/rfc/rfc5861#section-4>
    ///
    /// This filter is only called if cache is enabled.
    // 5xx HTTP status will be encoded as ErrorType::HTTPStatus(code)
    fn should_serve_stale(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
        error: Option<&Error>, // None when it is called during stale while revalidate
    ) -> bool {
        // A cache MUST NOT generate a stale response unless
        // it is disconnected
        // or doing so is explicitly permitted by the client or origin server
        // (e.g. headers or an out-of-band contract)
        error.is_some_and(|e| e.esource() == &ErrorSource::Upstream)
    }

    /// This filter is called when the request just established or reused a connection to the upstream
    ///
    /// This filter allows user to log timing and connection related info.
    async fn connected_to_upstream(
        &self,
        _session: &mut Session,
        _reused: bool,
        _peer: &HttpPeer,
        #[cfg(unix)] _fd: std::os::unix::io::RawFd,
        #[cfg(windows)] _sock: std::os::windows::io::RawSocket,
        _digest: Option<&Digest>,
        _ctx: &mut Self::CTX,
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        Ok(())
    }

    /// This callback is invoked every time request related error log needs to be generated
    ///
    /// Users can define what is important to be written about this request via the returned string.
    fn request_summary(&self, session: &Session, _ctx: &Self::CTX) -> String {
        session.as_ref().request_summary()
    }

    /// Whether the request should be used to invalidate(delete) the HTTP cache
    ///
    /// - `true`: this request will be used to invalidate the cache.
    /// - `false`: this request is a treated as a normal request
    fn is_purge(&self, _session: &Session, _ctx: &Self::CTX) -> bool {
        false
    }

    /// This filter is called after the proxy cache generates the downstream response to the purge
    /// request (to invalidate or delete from the HTTP cache), based on the purge status, which
    /// indicates whether the request succeeded or failed.
    ///
    /// The filter allows the user to modify or replace the generated downstream response.
    /// If the filter returns `Err`, the proxy will instead send a 500 response.
    fn purge_response_filter(
        &self,
        _session: &Session,
        _ctx: &mut Self::CTX,
        _purge_status: PurgeStatus,
        _purge_response: &mut std::borrow::Cow<'static, ResponseHeader>,
    ) -> Result<()> {
        Ok(())
    }
}

/// Context struct returned by `fail_to_proxy`.
pub struct FailToProxy {
    pub error_code: u16,
    pub can_reuse_downstream: bool,
}

#[cfg(test)]
mod tests {
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
    fn disposition_defaults_to_ordinary() {
        let io = tokio_test::io::Builder::new().build();
        let session = Session::new_h1(Box::new(io));
        assert_eq!(
            DefaultsOnly.upstream_request_body_disposition(&session, &()),
            UpstreamRequestBodyDisposition::Ordinary
        );
    }

    #[test]
    fn retry_allowed_defaults_to_true() {
        let io = tokio_test::io::Builder::new().build();
        let session = Session::new_h1(Box::new(io));
        assert!(DefaultsOnly.request_retry_allowed(&session, &()));
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
            _end_of_stream: bool,
            ctx: &mut Self::CTX,
        ) -> Result<()> {
            *ctx = true;
            Ok(())
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
            .request_body_filter_action(&mut session, &mut body, false, &mut ctx)
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
}
