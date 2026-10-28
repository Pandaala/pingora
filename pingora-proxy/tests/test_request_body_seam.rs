// Self-contained integration tests for the canonical request-body transport
// interface. Deliberately does NOT use tests/utils: that harness needs a
// local openresty. Every upstream here is a scripted tokio TCP listener.
//
// Assertions about what the proxy put on the wire go through the upstream
// event `Recorder` (see `seam::harness`) rather than through echoed response
// headers or shared counters: an echoed header proves what the APPLICATION
// saw, which is a different claim.
//
// This file is only the matrix instantiation. Everything else lives under
// `tests/seam/`, which cargo treats as a plain module tree rather than as
// further test binaries (only `.rs` files directly under `tests/` become
// binaries):
//   seam/harness.rs   -- proxy, scripted upstreams, event recorder, clients
//   seam/scenarios.rs -- shared bodies, one run per transport combination
//   seam/single.rs    -- tests that are inherently one combination
//
// The four cells below are the point of the file: this fork's request-body
// seam has a separate upstream pump per upstream protocol and a separate
// session per downstream protocol, and every hand-written pair of tests it
// grew covered two of the four. `cargo test h1_to_h2` filters to one cell,
// `cargo test trailer_terminate` to one scenario across all of them.

// An ignored `wait_for`/`expect_none` asserts nothing at all, and both return a
// `#[must_use]` `Result`. Denying the lint makes a dropped wait a compile
// error rather than a silently vacuous test.
#![deny(unused_must_use)]

mod seam;

// `Down::*` / `Up::*` below are resolved inside each generated cell module,
// which imports them itself -- hence no `use` here.
matrix!(h1_to_h1, Down::H1, Up::H1);
matrix!(h1_to_h2, Down::H1, Up::H2);
matrix!(h2c_to_h1, Down::H2c, Up::H1);
matrix!(h2c_to_h2, Down::H2c, Up::H2);
