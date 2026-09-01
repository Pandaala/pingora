//! The request-body seam test harness.
//!
//! Layout:
//! - [`harness`] -- the proxy under test, the scripted upstreams and their
//!   event recorder, and the downstream clients. Asserts nothing.
//! - [`scenarios`] -- shared test bodies, each taking a [`Combo`] and run once
//!   per downstream/upstream transport combination by the `matrix!` macro in
//!   `tests/test_request_body_seam.rs`.
//! - [`single`] -- tests that are inherently ONE combination (the shape they
//!   drive only exists on one transport), kept as ordinary `#[test]` fns.
//!
//! Why a matrix at all: this fork's request-body seam touches four separate
//! code paths (`proxy_h1`/`proxy_h2` upstream pumps x H1/H2 downstream
//! sessions), and the tests it grew were hand-duplicated per pair -- which
//! means the combinations nobody thought to write by hand were never covered.
//! The H1-downstream -> H2-upstream cell is where a real defect hid here
//! before. Generating one test per combination from one body removes both the
//! duplication and the blind spots, and keeps the whole thing a
//! seconds-not-minutes regression to run after a rebase onto upstream Pingora.

pub mod harness;
pub mod scenarios;
pub mod single;

use harness::{
    spawn_scripted_h2_upstream, spawn_scripted_upstream, ExercisedUpstream, H2UpstreamStep,
    Recorder, UpstreamStep, OK_KEEPALIVE,
};

/// The transport the CLIENT speaks to the proxy.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Down {
    /// Plain HTTP/1.1, the `h1` listener.
    H1,
    /// Prior-knowledge HTTP/2 cleartext, the `h2c` listener.
    H2c,
}

/// The transport the PROXY speaks to the upstream, selected per request by the
/// `x-h2` header (see `SeamProxy::upstream_peer`). It is what picks the
/// `proxy_h1` or `proxy_h2` pump.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Up {
    H1,
    H2,
}

/// One cell of the transport matrix.
#[derive(Clone, Copy, Debug)]
pub struct Combo {
    pub down: Down,
    pub up: Up,
}

/// A protocol-neutral upstream script step.
///
/// A shared body says what it wants the origin to DO; the combination decides
/// how that is spelled on H1 or H2. Where the two transports cannot mean the
/// same thing the body must branch instead of naming a step here -- see the
/// `match combo.up` arms in [`scenarios`].
#[derive(Clone, Copy)]
pub enum Step {
    /// Never answer; observe (and record) the proxy giving up on the request.
    HangObservingCancel,
    /// Consume the WHOLE request body, then answer 200. The recorded request
    /// framing and body length are therefore a precondition of the client's
    /// 200 rather than a race against it.
    DrainThenOk200,
    /// Answer 200 immediately, then record anything else that arrives.
    /// For requests the proxy declared bodyless, where there is no body
    /// framing to parse.
    Ok200ThenRecordExtra,
}

impl Combo {
    /// The proxy listener a client of this combination connects to.
    pub fn down_addr(&self) -> String {
        let ports = harness::init();
        match self.down {
            Down::H1 => ports.h1_addr(),
            Down::H2c => ports.h2c_addr(),
        }
    }

    /// A `reqwest` client speaking this combination's downstream transport.
    ///
    /// h2c needs prior knowledge: the proxy's h2c listener never speaks H1, so
    /// a default client would send an HTTP/1.1 request at it.
    pub fn client(&self) -> reqwest::Client {
        let builder = reqwest::Client::builder();
        match self.down {
            Down::H1 => builder.build().unwrap(),
            Down::H2c => builder.http2_prior_knowledge().build().unwrap(),
        }
    }

    /// Whether a request through this combination must carry `x-h2`.
    pub fn upstream_is_h2(&self) -> bool {
        self.up == Up::H2
    }

    /// Spawn the upstream this combination wants, returning its port, a handle
    /// on its event log, and the vacuity guard.
    ///
    /// The guard must be kept alive for the whole test: dropping it is what
    /// asserts the upstream was reached at all.
    pub fn spawn(&self, script: &[Step]) -> (u16, Recorder, ExercisedUpstream) {
        let upstream = match self.up {
            Up::H1 => spawn_scripted_upstream(script.iter().map(Step::h1).collect()),
            Up::H2 => spawn_scripted_h2_upstream(script.iter().map(Step::h2).collect()),
        };
        let (port, rec) = (upstream.port(), upstream.rec().clone());
        (port, rec, upstream)
    }

    /// Spawn an H1 upstream regardless of the combination.
    ///
    /// For the second, "must never be dialled" upstream a few scenarios point a
    /// follow-up request at: its transport is irrelevant to the claim, and
    /// making it H1 keeps the claim (`connections() == 0`) identical in every
    /// cell.
    pub fn spawn_unused_h1(&self) -> (u16, Recorder, ExercisedUpstream) {
        let upstream = spawn_scripted_upstream(vec![UpstreamStep::Respond(OK_KEEPALIVE)]);
        upstream.expect_unused();
        let (port, rec) = (upstream.port(), upstream.rec().clone());
        (port, rec, upstream)
    }
}

impl Step {
    fn h1(&self) -> UpstreamStep {
        match self {
            Step::HangObservingCancel => UpstreamStep::HangObservingClose,
            Step::DrainThenOk200 => UpstreamStep::RespondAfterBody(OK_KEEPALIVE),
            Step::Ok200ThenRecordExtra => UpstreamStep::RespondThenRecordExtra(OK_KEEPALIVE),
        }
    }

    fn h2(&self) -> H2UpstreamStep {
        match self {
            // `Hang` drains the request stream first, so the proxy's
            // RST_STREAM is recorded before the step parks.
            Step::HangObservingCancel => H2UpstreamStep::Hang,
            // `EchoRequestEos` drains the request stream to its end before
            // responding, i.e. exactly "answer only once the body is in".
            Step::DrainThenOk200 => H2UpstreamStep::EchoRequestEos,
            // There is no h2 analogue of "unframed trailing bytes": every DATA
            // frame is recorded by the drain either way.
            Step::Ok200ThenRecordExtra => H2UpstreamStep::EchoRequestEos,
        }
    }
}

/// Announce that this combination is not exercising the scenario, and return.
///
/// Silence is the failure mode this guards against: a body that quietly
/// returned for two of its four cells looks exactly like one that passed them.
/// The `eprintln!` shows up under `cargo test -- --nocapture`, and the call
/// also stands the [`ExercisedUpstream`] vacuity guard down so an opt-out
/// cannot be reported as a vacuous test.
#[macro_export]
macro_rules! skip_combo {
    ($combo:expr, $reason:expr) => {{
        $crate::seam::harness::note_combination_skipped();
        // The enclosing function's path, so the line names the SCENARIO rather
        // than the module every scenario shares.
        fn scenario() {}
        fn path_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = path_of(scenario);
        eprintln!(
            "SKIP {} for {:?} -> {:?}: {}",
            name.strip_suffix("::scenario").unwrap_or(name),
            $combo.down,
            $combo.up,
            $reason
        );
        return;
    }};
}

/// Instantiate every shared scenario body for one transport combination.
///
/// One module per cell, so the generated names read as
/// `h1_to_h2::terminate_is_prompt_and_cancels_the_upstream`: `cargo test
/// h1_to_h2` filters to a cell and `cargo test terminate_is_prompt` filters to
/// a scenario across all cells.
///
/// Expand one `#[test]` per shared body inside an already-generated cell
/// module. Split out of [`matrix!`] only because a `macro_rules!` cannot nest a
/// `$name` capture inside its own expansion.
#[macro_export]
macro_rules! cell_tests {
    ($($name:ident),+ $(,)?) => {
        $(
            #[test]
            fn $name() {
                $crate::seam::harness::enter_test();
                scenarios::$name(C)
            }
        )+
    };
}

#[macro_export]
macro_rules! matrix {
    ($cell:ident, $down:expr, $up:expr) => {
        mod $cell {
            use $crate::seam::{scenarios, Combo, Down, Up};

            const C: Combo = Combo {
                down: $down,
                up: $up,
            };

            // Every shared body, once per cell. Adding a scenario means adding
            // one line here and it is covered in all four cells at once, which
            // is the whole point of the mechanism.
            $crate::cell_tests! {
                terminate_is_prompt_and_cancels_the_upstream,
                mid_body_terminate_is_not_reused,
                trailer_terminate_is_not_reused,
                terminate_finishes_an_unfinished_local_reply,
                bodyless_with_a_real_body_fails_closed,
                cl0_without_end_stream_sends_one_eos,
                streamed_disposition_rewrites_upstream_framing,
                upstream_graceful_goaway_finishes_in_flight_and_is_not_reused,
                upstream_error_goaway_fails_the_request_without_a_silent_retry,
            }
        }
    };
}
