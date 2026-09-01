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
use bytes::Bytes;
use pingora_http::ResponseHeader;

fn limits(bytes: usize, chunks: usize, events: usize, metadata: usize) -> ResponseHeadHoldLimits {
    ResponseHeadHoldLimits::new_for_test(bytes, chunks, events, metadata, Duration::from_secs(30))
}

fn hold(limits: ResponseHeadHoldLimits) -> ResponseHeadCommitPlan {
    ResponseHeadCommitPlan::Hold(ResponseHeadHoldPlan::new_for_test(limits))
}

fn header() -> HttpTask {
    HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false)
}

fn body(value: &'static [u8]) -> HttpTask {
    HttpTask::Body(Some(Bytes::from_static(value)), false)
}

fn body_values(tasks: &[HttpTask]) -> Vec<&[u8]> {
    tasks
        .iter()
        .filter_map(|task| match task {
            HttpTask::Body(Some(body), _) => Some(body.as_ref()),
            _ => None,
        })
        .collect()
}

#[test]
fn replacement_has_a_bounded_owned_shape() {
    let replacement = ResponseHeadReplacement::new(
        Box::new(ResponseHeader::build(403, None).unwrap()),
        vec![Bytes::from_static(b"denied")],
    );
    assert_eq!(replacement.header().status.as_u16(), 403);
    assert_eq!(replacement.body(), &[Bytes::from_static(b"denied")]);

    let (header, body) = replacement.into_parts();
    assert_eq!(header.status.as_u16(), 403);
    assert_eq!(body, vec![Bytes::from_static(b"denied")]);
}

#[test]
fn informational_replacement_is_not_a_complete_local_response() {
    let mut barrier = ResponseHeadBarrier::default();
    barrier
        .select(hold(limits(32, 4, 4, HEAD_METADATA_BASE_COST)))
        .unwrap();
    let replacement = ResponseHeadReplacement::new(
        Box::new(ResponseHeader::build(101, None).unwrap()),
        Vec::new(),
    );
    let mut tasks = Vec::new();
    assert!(matches!(
        barrier.replace(&mut tasks, 0, replacement),
        Err(ResponseHeadBarrierFailure::Boundary(
            ResponseHeadBoundary::Unsupported
        ))
    ));
    assert!(barrier.is_holding());
    assert!(tasks.is_empty());
}

#[test]
fn public_usage_exposes_only_aggregate_counters() {
    let usage = ResponseHeadUsage::new(10, 8, 2, 3, 64, 5, Duration::from_millis(7));
    assert_eq!(usage.input_bytes(), 10);
    assert_eq!(usage.output_bytes(), 8);
    assert_eq!(usage.nonempty_chunks(), 2);
    assert_eq!(usage.events(), 3);
    assert_eq!(usage.metadata_bytes(), 64);
    assert_eq!(usage.work_units(), 5);
    assert_eq!(usage.held_for(), Duration::from_millis(7));
}

#[test]
fn boundary_labels_cover_the_accepted_categories() {
    let cases = [
        (ResponseHeadBoundary::Unsupported, "unsupported"),
        (ResponseHeadBoundary::InputLimit, "input-limit"),
        (ResponseHeadBoundary::OutputLimit, "output-limit"),
        (ResponseHeadBoundary::ChunkLimit, "chunk-limit"),
        (ResponseHeadBoundary::EventLimit, "event-limit"),
        (ResponseHeadBoundary::MetadataLimit, "metadata-limit"),
        (ResponseHeadBoundary::WorkLimit, "work-limit"),
        (ResponseHeadBoundary::Timeout, "timeout"),
        (
            ResponseHeadBoundary::CleanTerminalWithoutDecision,
            "clean-terminal-without-decision",
        ),
        (ResponseHeadBoundary::SourceFailed, "source-failed"),
        (ResponseHeadBoundary::ApplicationFail, "application-fail"),
        (
            ResponseHeadBoundary::ApplicationTerminate,
            "application-terminate",
        ),
    ];
    for (boundary, label) in cases {
        assert_eq!(boundary.as_str(), label);
    }
}

#[test]
fn outcome_preserves_the_failed_boundary_category() {
    assert_eq!(
        ResponseHeadOutcome::Failed(ResponseHeadBoundary::Timeout),
        ResponseHeadOutcome::Failed(ResponseHeadBoundary::Timeout)
    );
    assert_ne!(
        ResponseHeadOutcome::Immediate,
        ResponseHeadOutcome::Released
    );
}

#[test]
fn default_state_retains_nothing_and_awaits_final_head() {
    let barrier = ResponseHeadBarrier::default();
    assert!(!barrier.is_holding());
    assert!(barrier.retained_usage().is_none());
    assert!(matches!(
        barrier.state,
        ResponseHeadBarrierState::AwaitingFinalHead
    ));
}

#[test]
fn body_cost_counts_bytes_chunk_and_event() {
    let usage = ResponseHeadRetentionUsage::for_tasks(&[body(b"abc")]).unwrap();
    assert_eq!(
        usage,
        ResponseHeadRetentionUsage {
            output_bytes: 3,
            nonempty_chunks: 1,
            events: 1,
            ..ResponseHeadRetentionUsage::default()
        }
    );
}

#[test]
fn empty_body_counts_event_but_not_chunk_or_bytes() {
    let usage = ResponseHeadRetentionUsage::for_tasks(&[HttpTask::Body(Some(Bytes::new()), false)])
        .unwrap();
    assert_eq!(usage.output_bytes, 0);
    assert_eq!(usage.nonempty_chunks, 0);
    assert_eq!(usage.events, 1);
}

#[test]
fn header_cost_counts_fields_and_metadata_overhead() {
    let mut response = ResponseHeader::build(200, None).unwrap();
    response.insert_header("x-test", "abc").unwrap();
    let usage =
        ResponseHeadRetentionUsage::for_tasks(&[HttpTask::Header(Box::new(response), false)])
            .unwrap();
    assert_eq!(usage.events, 1);
    assert_eq!(
        usage.metadata_bytes,
        HEAD_METADATA_BASE_COST + "x-test".len() + 3 + HEADER_FIELD_METADATA_OVERHEAD
    );
}

#[test]
fn trailer_cost_counts_metadata() {
    let mut trailers = http::HeaderMap::new();
    trailers.insert("x-end", http::HeaderValue::from_static("yes"));
    let usage =
        ResponseHeadRetentionUsage::for_tasks(&[HttpTask::Trailer(Some(Box::new(trailers)))])
            .unwrap();
    assert_eq!(usage.events, 1);
    assert_eq!(
        usage.metadata_bytes,
        TRAILER_METADATA_BASE_COST + "x-end".len() + 3 + HEADER_FIELD_METADATA_OVERHEAD
    );
}

#[test]
fn checked_usage_addition_rejects_overflow() {
    let left = ResponseHeadRetentionUsage {
        output_bytes: usize::MAX,
        ..ResponseHeadRetentionUsage::default()
    };
    let right = ResponseHeadRetentionUsage {
        output_bytes: 1,
        ..ResponseHeadRetentionUsage::default()
    };
    assert!(left.checked_add(right).is_err());
}

#[test]
fn each_limit_accepts_exact_boundary() {
    let usage = ResponseHeadRetentionUsage {
        output_bytes: 3,
        nonempty_chunks: 1,
        events: 2,
        metadata_bytes: HEAD_METADATA_BASE_COST,
        ..ResponseHeadRetentionUsage::default()
    };
    assert!(usage
        .ensure_fits(limits(3, 1, 2, HEAD_METADATA_BASE_COST))
        .is_ok());
}

#[test]
fn each_limit_rejects_plus_one() {
    let baseline = ResponseHeadRetentionUsage {
        output_bytes: 3,
        nonempty_chunks: 1,
        events: 2,
        metadata_bytes: 4,
        ..ResponseHeadRetentionUsage::default()
    };
    assert!(baseline.ensure_fits(limits(2, 1, 2, 4)).is_err());
    assert!(baseline.ensure_fits(limits(3, 0, 2, 4)).is_err());
    assert!(baseline.ensure_fits(limits(3, 1, 1, 4)).is_err());
    assert!(baseline.ensure_fits(limits(3, 1, 2, 3)).is_err());
}

#[test]
fn holding_retains_tasks_across_submissions() {
    let mut barrier = ResponseHeadBarrier::default();
    barrier
        .select(hold(limits(32, 4, 4, HEAD_METADATA_BASE_COST)))
        .unwrap();

    let mut first = vec![header()];
    assert_eq!(
        barrier
            .capture_or_release(&mut first, 0, false, false)
            .unwrap(),
        ResponseHeadBarrierOutput::Held
    );
    assert!(first.is_empty());

    let mut second = vec![body(b"one")];
    assert_eq!(
        barrier
            .capture_or_release(&mut second, 0, false, false)
            .unwrap(),
        ResponseHeadBarrierOutput::Held
    );
    assert!(second.is_empty());
    assert_eq!(barrier.retained_usage().unwrap().events, 2);
}

#[test]
fn release_appends_current_after_all_held_tasks() {
    let mut barrier = ResponseHeadBarrier::default();
    barrier
        .select(hold(limits(32, 4, 4, HEAD_METADATA_BASE_COST)))
        .unwrap();
    let mut header_batch = vec![header()];
    barrier
        .capture_or_release(&mut header_batch, 0, false, false)
        .unwrap();
    let mut first_body = vec![body(b"one")];
    barrier
        .capture_or_release(&mut first_body, 0, false, false)
        .unwrap();

    let mut release_batch = vec![body(b"two")];
    assert_eq!(
        barrier
            .capture_or_release(&mut release_batch, 0, true, false)
            .unwrap(),
        ResponseHeadBarrierOutput::PrepareFrom(0)
    );
    assert!(matches!(release_batch[0], HttpTask::Header(..)));
    assert_eq!(
        body_values(&release_batch),
        vec![b"one".as_slice(), b"two".as_slice()]
    );
    assert!(!barrier.is_holding());
}

#[test]
fn failed_task_is_never_retained() {
    let mut barrier = ResponseHeadBarrier::default();
    barrier
        .select(hold(limits(32, 4, 4, HEAD_METADATA_BASE_COST)))
        .unwrap();
    let mut header_batch = vec![header()];
    barrier
        .capture_or_release(&mut header_batch, 0, false, false)
        .unwrap();

    let mut failed = vec![HttpTask::Failed(Error::explain(InternalError, "upstream"))];
    let failure = barrier
        .capture_or_release(&mut failed, 0, false, false)
        .unwrap_err();
    let ResponseHeadBarrierFailure::Source(error) = failure else {
        panic!("source failure must preserve the original error")
    };
    assert!(error.to_string().contains("upstream"));
    assert!(failed.is_empty());
    assert!(
        barrier.is_holding(),
        "the pipeline still needs the Hold usage before recording SourceFailed"
    );
}

#[test]
fn clean_terminal_without_release_aborts_and_clears_tasks() {
    let mut barrier = ResponseHeadBarrier::default();
    barrier
        .select(hold(limits(32, 4, 4, HEAD_METADATA_BASE_COST)))
        .unwrap();
    let mut header_batch = vec![header()];
    barrier
        .capture_or_release(&mut header_batch, 0, false, false)
        .unwrap();

    let mut terminal = vec![HttpTask::Body(None, true)];
    let failure = barrier
        .capture_or_release(&mut terminal, 0, false, true)
        .unwrap_err();
    assert!(matches!(
        failure,
        ResponseHeadBarrierFailure::Boundary(ResponseHeadBoundary::CleanTerminalWithoutDecision)
    ));
    assert!(terminal.is_empty());
    assert!(barrier.is_holding());
}

#[test]
fn overflow_does_not_partially_retain_candidate() {
    let mut barrier = ResponseHeadBarrier::default();
    barrier.select(hold(limits(2, 1, 2, 0))).unwrap();
    let mut first = vec![body(b"ok")];
    barrier
        .capture_or_release(&mut first, 0, false, false)
        .unwrap();

    let mut oversized = vec![body(b"x")];
    assert!(barrier
        .capture_or_release(&mut oversized, 0, false, false)
        .is_err());
    assert_eq!(body_values(&oversized), vec![b"x".as_slice()]);
    assert!(barrier.is_holding());
}

#[tokio::test(start_paused = true)]
async fn release_at_or_after_deadline_fails_and_drops_the_prefix() {
    let mut barrier = ResponseHeadBarrier::default();
    barrier
        .select(hold(ResponseHeadHoldLimits::new_for_test(
            32,
            4,
            4,
            HEAD_METADATA_BASE_COST,
            Duration::from_millis(25),
        )))
        .unwrap();
    let deadline = barrier.deadline().expect("Hold must expose its deadline");

    let mut header_batch = vec![header()];
    barrier
        .capture_or_release(&mut header_batch, 0, false, false)
        .unwrap();
    tokio::time::sleep_until(deadline).await;

    let mut release_batch = vec![body(b"late")];
    let failure = barrier
        .capture_or_release(&mut release_batch, 0, true, false)
        .unwrap_err();
    assert!(matches!(
        failure,
        ResponseHeadBarrierFailure::Boundary(ResponseHeadBoundary::Timeout)
    ));
    assert!(release_batch.is_empty());
    assert!(barrier.is_holding());
}

#[test]
fn pump_timeout_aborts_and_releases_retained_tasks() {
    let mut barrier = ResponseHeadBarrier::default();
    barrier
        .select(hold(limits(32, 4, 4, HEAD_METADATA_BASE_COST)))
        .unwrap();
    let mut header_batch = vec![header()];
    barrier
        .capture_or_release(&mut header_batch, 0, false, false)
        .unwrap();

    let error = barrier.timeout();
    assert!(error.to_string().contains("absolute deadline exceeded"));
    assert!(!barrier.is_holding());
    assert!(barrier.retained_usage().is_none());
}
