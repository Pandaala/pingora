//! Controls for the observation channel used by the Bodyless negative tests.

use super::*;
use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::ReadBuf;

const HEAD: &[u8] = b"POST / HTTP/1.1\r\nHost: t\r\n\r\n";

// Each entry is exactly one read, independent of TCP buffering or scheduling.
struct ScriptedIo {
    reads: VecDeque<Vec<u8>>,
    response: Vec<u8>,
    fail_write: bool,
}

impl AsyncRead for ScriptedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(bytes) = self.reads.pop_front() {
            assert!(bytes.len() <= buf.remaining());
            buf.put_slice(&bytes);
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ScriptedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.fail_write {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        self.response.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

async fn assert_extra_bytes(reads: Vec<Vec<u8>>, fail_write: bool, lengths: &[usize]) {
    let mut io = ScriptedIo {
        reads: reads.into(),
        response: Vec::new(),
        fail_write,
    };
    let rec = Recorder::new();
    let conn = rec.accept_conn();
    let counter = AtomicUsize::new(0);
    serve_scripted_h1_connection(
        &mut io,
        conn,
        &rec,
        &counter,
        &[UpstreamStep::RespondThenRecordExtra(OK_KEEPALIVE)],
    )
    .await;

    let mut expected = vec![
        UpEvent::ConnAccepted { conn },
        UpEvent::ReqHeaders {
            conn,
            stream: 0,
            headers_eos: true,
            content_length: None,
            transfer_encoding: None,
        },
    ];
    expected.extend(lengths.iter().map(|&len| UpEvent::ReqData {
        conn,
        stream: 0,
        len,
        end_stream: false,
    }));
    if !fail_write {
        expected.push(UpEvent::PeerHalfClose { conn });
        assert_eq!(io.response, OK_KEEPALIVE);
    } else {
        assert!(io.response.is_empty());
    }
    let actual: Vec<_> = rec
        .log
        .lock()
        .unwrap()
        .iter()
        .map(|r| r.event.clone())
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(rec.body_bytes(), lengths.iter().sum::<usize>());
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert!(io.reads.is_empty());
}

#[tokio::test]
async fn extra_recorder_counts_header_read_surplus() {
    assert_extra_bytes(vec![[HEAD, b"hello"].concat()], false, &[5]).await;
}

#[tokio::test]
async fn extra_recorder_counts_separate_reads() {
    assert_extra_bytes(vec![HEAD.to_vec(), b"hello".to_vec()], false, &[5]).await;
}

#[tokio::test]
async fn extra_recorder_combines_surplus_and_later_reads() {
    assert_extra_bytes(
        vec![[HEAD, b"hello"].concat(), b"world".to_vec()],
        false,
        &[5, 5],
    )
    .await;
}

#[tokio::test]
async fn extra_recorder_header_only_has_no_data_events() {
    assert_extra_bytes(vec![HEAD.to_vec()], false, &[]).await;
}

#[tokio::test]
async fn extra_recorder_preserves_surplus_when_response_write_fails() {
    assert_extra_bytes(vec![[HEAD, b"hello"].concat()], true, &[5]).await;
}
