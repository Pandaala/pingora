//! Hold must retain its absolute deadline while the duplex pump uploads.

use async_trait::async_trait;
use bytes::Bytes;
use pingora_core::{
    apps::HttpServerApp, protocols::http::ServerSession, server::configuration::ServerConf,
    upstreams::peer::HttpPeer,
};
use pingora_error::{Error, ErrorType, Result};
use pingora_http::ResponseHeader;
use pingora_proxy::{
    HttpProxy, ProxyHttp, RequestBodyEvent, ResponseBodySink, ResponseHeadBoundary,
    ResponseHeadBoundaryAction, ResponseHeadCommitPlan, ResponseHeadHoldLimits,
    ResponseHeadOutcome, ResponseHeadSource, ResponseHeadUsage, Session,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
};

const HOLD: Duration = Duration::from_millis(300);
const GUARD: Duration = Duration::from_secs(3);
const UPLOAD: &[u8] = &[b'x'; 128];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Hook,
    Capacity(u32),
    Progress,
    Idle,
}

#[derive(Default)]
struct Observation {
    outcomes: Vec<ResponseHeadOutcome>,
    boundaries: Vec<ResponseHeadBoundary>,
    commits: usize,
    upstream_attempts: usize,
    hook_bytes: usize,
    finished: bool,
}

#[derive(Default)]
struct Signals {
    selected: Notify,
    hook: Notify,
    finished: Notify,
    observation: Mutex<Observation>,
}

struct UploadProxy {
    port: u16,
    h2: bool,
    mode: Mode,
    signals: Arc<Signals>,
}

#[async_trait]
impl ProxyHttp for UploadProxy {
    type CTX = ();
    fn new_ctx(&self) {}
    async fn upstream_peer(&self, _: &mut Session, _: &mut ()) -> Result<Box<HttpPeer>> {
        self.signals.observation.lock().unwrap().upstream_attempts += 1;
        let mut peer = HttpPeer::new(("127.0.0.1", self.port), false, String::new());
        if self.h2 {
            peer.options.set_http_version(2, 2);
        }
        peer.options.read_timeout = Some(Duration::from_secs(60));
        peer.options.write_timeout = Some(Duration::from_secs(60));
        Ok(Box::new(peer))
    }
    fn response_head_may_hold(&self, _: &Session, _: &()) -> bool {
        true
    }
    fn response_head_commit_plan(
        &self,
        _: &Session,
        _: ResponseHeadSource,
        _: &ResponseHeader,
        _: &(),
    ) -> Result<ResponseHeadCommitPlan> {
        self.signals.selected.notify_one();
        Ok(ResponseHeadCommitPlan::hold(ResponseHeadHoldLimits::new(
            1024, 1024, 16, 32, 4096, 32, HOLD,
        )))
    }
    fn response_head_will_commit(&self, _: &Session, _: &ResponseHeader, _: &mut ()) -> Result<()> {
        self.signals.observation.lock().unwrap().commits += 1;
        Ok(())
    }
    fn response_head_hold_boundary(
        &self,
        _: &Session,
        boundary: ResponseHeadBoundary,
        _: &mut (),
    ) -> ResponseHeadBoundaryAction {
        self.signals
            .observation
            .lock()
            .unwrap()
            .boundaries
            .push(boundary);
        ResponseHeadBoundaryAction::Fail(Error::explain(
            ErrorType::HTTPStatus(504),
            "hold upload deadline",
        ))
    }
    fn response_head_hold_outcome(
        &self,
        _: &Session,
        outcome: ResponseHeadOutcome,
        _: ResponseHeadUsage,
        _: &mut (),
    ) {
        self.signals
            .observation
            .lock()
            .unwrap()
            .outcomes
            .push(outcome);
    }
    async fn request_body_filter(
        &self,
        _: &mut Session,
        body: &mut Option<Bytes>,
        _: RequestBodyEvent,
        _: &mut (),
    ) -> Result<()> {
        if let Some(body) = body.as_ref().filter(|b| !b.is_empty()) {
            self.signals.observation.lock().unwrap().hook_bytes += body.len();
            self.signals.hook.notify_one();
            if self.mode == Mode::Hook {
                std::future::pending::<()>().await;
            }
        }
        Ok(())
    }
    async fn upstream_response_body_filter(
        &self,
        _: &mut Session,
        body: &mut Option<Bytes>,
        _: bool,
        sink: &mut ResponseBodySink,
        _: &mut (),
    ) -> Result<Option<Duration>> {
        if self.mode == Mode::Progress && body.as_ref().is_some_and(|b| !b.is_empty()) {
            assert!(sink.release_response_head());
        }
        Ok(None)
    }
    async fn logging(&self, _: &mut Session, _: Option<&Error>, _: &mut ()) {
        self.signals.observation.lock().unwrap().finished = true;
        self.signals.finished.notify_one();
    }
}

async fn read_head(io: &mut TcpStream) {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        head.push(io.read_u8().await.unwrap());
    }
}

async fn exercise(h2: bool, mode: Mode) {
    let origin_stop = Arc::new(Notify::new());
    let stop = origin_stop.clone();
    let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = origin.local_addr().unwrap().port();
    let origin_task = tokio::spawn(async move {
        let (mut io, _) = origin.accept().await.unwrap();
        if h2 {
            let mut builder = h2::server::Builder::new();
            if let Mode::Capacity(credit) = mode {
                builder.initial_window_size(credit);
            }
            let mut conn = builder.handshake::<_, Bytes>(io).await.unwrap();
            let (req, mut respond) = conn.accept().await.unwrap().unwrap();
            let mut body = req.into_body();
            let mut send = respond
                .send_response(
                    http::Response::builder()
                        .status(200)
                        .header("content-length", "1")
                        .body(())
                        .unwrap(),
                    false,
                )
                .unwrap();
            let driver = tokio::spawn(async move { while conn.accept().await.is_some() {} });
            let mut received = Vec::new();
            if mode == Mode::Progress {
                while let Some(chunk) = body.data().await {
                    received.extend_from_slice(&chunk.unwrap());
                }
                assert_eq!(received, UPLOAD);
                send.send_data(Bytes::from_static(b"z"), true).unwrap();
                // Keep driving H2 until the proxy has consumed the queued DATA.
                stop.notified().await;
            } else {
                if let Mode::Capacity(credit) = mode {
                    while received.len() < credit as usize {
                        received.extend_from_slice(&body.data().await.unwrap().unwrap());
                    }
                    assert_eq!(received, UPLOAD[..credit as usize]);
                }
                let reason = futures::future::poll_fn(|cx| send.poll_reset(cx))
                    .await
                    .unwrap();
                assert_eq!(reason, h2::Reason::CANCEL);
            }
            driver.abort();
            received.len()
        } else {
            read_head(&mut io).await;
            io.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n")
                .await
                .unwrap();
            if mode == Mode::Progress {
                let mut received = vec![0; UPLOAD.len()];
                io.read_exact(&mut received).await.unwrap();
                assert_eq!(received, UPLOAD);
                io.write_all(b"z").await.unwrap();
                received.len()
            } else {
                let mut received = Vec::new();
                io.read_to_end(&mut received).await.unwrap();
                assert!(
                    received.is_empty(),
                    "cancelled hook must not forward upload bytes"
                );
                0
            }
        }
    });
    let downstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = TcpStream::connect(downstream.local_addr().unwrap())
        .await
        .unwrap();
    let (accepted, _) = downstream.accept().await.unwrap();
    let signals = Arc::new(Signals::default());
    let mut proxy = HttpProxy::new(
        UploadProxy {
            port,
            h2,
            mode,
            signals: signals.clone(),
        },
        Arc::new(ServerConf::default()),
    );
    proxy.handle_init_modules();
    let (_shutdown_tx, shutdown) = tokio::sync::watch::channel(false);
    let proxy_task = tokio::spawn(async move {
        Arc::new(proxy)
            .process_new_http(
                ServerSession::new_http1(Box::new(
                    pingora_core::protocols::l4::stream::Stream::from(accepted),
                )),
                &shutdown,
            )
            .await;
    });
    client
        .write_all(
            format!(
                "POST / HTTP/1.1\r\nHost: test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                UPLOAD.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    tokio::time::timeout(GUARD, signals.selected.notified())
        .await
        .expect("Hold selected before upload");
    if mode != Mode::Idle {
        client.write_all(UPLOAD).await.unwrap();
        tokio::time::timeout(GUARD, signals.hook.notified())
            .await
            .expect("upload hook entered");
    }
    tokio::time::timeout(GUARD, signals.finished.notified())
        .await
        .expect("Hold must terminate independently of the 60s peer timeout");
    origin_stop.notify_one();
    let mut wire = Vec::new();
    tokio::time::timeout(GUARD, client.read_to_end(&mut wire))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(GUARD, proxy_task)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(GUARD, origin_task)
        .await
        .expect("origin promptly cleaned up")
        .unwrap();
    let observed = signals.observation.lock().unwrap();
    assert!(observed.finished);
    if mode == Mode::Progress {
        assert_eq!(observed.commits, 1);
        assert_eq!(observed.outcomes, vec![ResponseHeadOutcome::Released]);
        assert!(observed.boundaries.is_empty());
        assert!(wire.starts_with(b"HTTP/1.1 200"));
        assert!(wire.ends_with(b"z"));
    } else {
        assert_eq!(observed.commits, 0);
        assert_eq!(
            observed.outcomes,
            vec![ResponseHeadOutcome::Failed(ResponseHeadBoundary::Timeout)]
        );
        assert_eq!(observed.boundaries, vec![ResponseHeadBoundary::Timeout]);
        assert!(
            !wire.starts_with(b"HTTP/1.1 200"),
            "held origin head must not escape"
        );
    }
    assert_eq!(observed.upstream_attempts, 1, "Hold must close retry");
    match mode {
        Mode::Progress => assert_eq!(observed.hook_bytes, UPLOAD.len()),
        Mode::Idle => assert_eq!(observed.hook_bytes, 0),
        Mode::Hook | Mode::Capacity(_) => {
            // TCP may split the upload before the first selected operation
            // stalls. Unconsumed fragments need not reach the hook on failure.
            assert!((1..=UPLOAD.len()).contains(&observed.hook_bytes));
        }
    }
}

#[tokio::test]
async fn hold_deadline_interrupts_upload_hooks() {
    for h2 in [false, true] {
        exercise(h2, Mode::Hook).await;
    }
}

#[tokio::test]
async fn hold_deadline_interrupts_h2_upload_capacity_waits() {
    for credit in [0, 32] {
        exercise(true, Mode::Capacity(credit)).await;
    }
}

#[tokio::test]
async fn hold_deadline_preserves_progressing_uploads() {
    for h2 in [false, true] {
        exercise(h2, Mode::Progress).await;
    }
}

#[tokio::test]
async fn hold_deadline_still_bounds_idle_responses() {
    for h2 in [false, true] {
        exercise(h2, Mode::Idle).await;
    }
}
