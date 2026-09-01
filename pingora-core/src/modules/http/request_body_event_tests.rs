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

#[derive(Default)]
struct BodyEventModule {
    last_event: Option<RequestBodyEvent>,
    /// Every event, in order. `last_event` alone cannot say whether a
    /// terminal event was delivered once, twice, or preceded by the wrong
    /// thing.
    events: Vec<RequestBodyEvent>,
}

#[async_trait]
impl HttpModule for BodyEventModule {
    async fn request_body_filter(
        &mut self,
        _body: &mut Option<Bytes>,
        event: RequestBodyEvent,
    ) -> Result<()> {
        self.last_event = Some(event);
        self.events.push(event);
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

struct BodyEventModuleBuilder;

impl HttpModuleBuilder for BodyEventModuleBuilder {
    fn init(&self) -> Module {
        Box::new(BodyEventModule::default())
    }
}

#[tokio::test]
async fn request_body_abandoned_event_reaches_modules() {
    let mut modules = HttpModules::new();
    modules.add_module(Box::new(BodyEventModuleBuilder));
    let mut ctx = modules.build_ctx();
    let mut body = None;

    ctx.request_body_filter(&mut body, RequestBodyEvent::Abandoned)
        .await
        .unwrap();

    assert_eq!(
        ctx.get::<BodyEventModule>().unwrap().last_event,
        Some(RequestBodyEvent::Abandoned)
    );
}

/// The negative direction of the test above: a module must be handed the
/// events it was given, unchanged and in order.
///
/// In particular a normal end of stream must arrive as `Complete`. Nothing
/// downstream of this dispatcher can recover the distinction once it is
/// lost, and every "exactly one terminal event" assertion in the proxy test
/// suites is satisfied by `Abandoned` too -- so a mislabelling here is
/// invisible everywhere else.
#[tokio::test]
async fn request_body_events_reach_modules_unchanged_and_in_order() {
    let mut modules = HttpModules::new();
    modules.add_module(Box::new(BodyEventModuleBuilder));
    let mut ctx = modules.build_ctx();

    let sequence = [
        RequestBodyEvent::Data,
        RequestBodyEvent::Data,
        RequestBodyEvent::Complete,
    ];
    for event in sequence {
        let mut body = None;
        ctx.request_body_filter(&mut body, event).await.unwrap();
    }

    assert_eq!(
        ctx.get::<BodyEventModule>().unwrap().events,
        sequence.to_vec(),
        "the module chain must not reclassify, drop or reorder request-body events"
    );
}

/// The two predicates the pumps and their applications branch on.
///
/// `is_terminal()` deliberately covers BOTH terminal variants, so it cannot
/// stand in for "the body completed"; `is_complete()` is the only predicate
/// that means the downstream transport's real end-of-stream was observed.
/// Confusing the two is a one-word edit with no other visible symptom.
#[test]
fn terminal_and_complete_classify_every_event() {
    assert!(!RequestBodyEvent::Data.is_terminal());
    assert!(RequestBodyEvent::Complete.is_terminal());
    assert!(
        RequestBodyEvent::Abandoned.is_terminal(),
        "an abandoned body still ends the delivery sequence, so a pump that only \
         finalizes on `is_terminal()` must still finalize here"
    );

    assert!(!RequestBodyEvent::Data.is_complete());
    assert!(RequestBodyEvent::Complete.is_complete());
    assert!(
        !RequestBodyEvent::Abandoned.is_complete(),
        "the bytes delivered before an abandonment are only a prefix: reporting them \
         as a complete body is what this variant exists to prevent"
    );

    // The `bool` conversion the pumps still use for their non-abandoning
    // call sites: a plain end-of-stream is a COMPLETION, never an
    // abandonment.
    assert_eq!(RequestBodyEvent::from(false), RequestBodyEvent::Data);
    assert_eq!(RequestBodyEvent::from(true), RequestBodyEvent::Complete);
}
