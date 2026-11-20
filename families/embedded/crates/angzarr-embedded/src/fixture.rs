//! Golden replay fixtures: an old log, kept exactly as it was stored, and
//! the states it must still replay to. With [`Registry::check`], what CI
//! runs so a missing upcaster or a drifted fold fails a build rather than a
//! replay in production.

use std::fmt::Debug;

use angzarr_proto::v1::{Cover, EventPage};

use super::domain::Domain;
use super::registry::Registry;
use super::stream::Stream;
use angzarr_router::PageContext;

/// An old log, kept as it was stored, and the states it must still replay
/// to.
pub struct ReplayFixture<'a, S> {
    /// Named in failures.
    pub name: &'a str,
    pub stream: Cover,
    /// The pages as stored, at whatever types they were written.
    pub log: Vec<EventPage>,
    /// The state just after each listed sequence.
    pub states: Vec<(u64, S)>,
}

/// Replay every fixture through the registry's upcaster for
/// `D`, and compare each listed state. Every failure is listed, one line
/// each, naming its fixture (and the type URL, for a page that does not
/// read).
pub fn check_replay_fixtures<D>(
    registry: &Registry,
    fixtures: &[ReplayFixture<'_, D::State>],
) -> Result<(), Vec<String>>
where
    D: Domain,
    D::State: PartialEq + Debug,
{
    let mut failures = Vec::new();
    for fixture in fixtures {
        let name = fixture.name;
        let stream = match Stream::<D>::replay(fixture.stream.clone(), &fixture.log, registry) {
            Ok(stream) => stream,
            Err(error) => {
                failures.push(format!("{name}: {error}"));
                continue;
            }
        };
        // The state just after each event: the replayed events folded
        // again, one at a time, from a fresh state.
        let mut state = registry.fresh::<D>();
        let states: Vec<(u64, D::State)> = stream
            .events()
            .iter()
            .map(|envelope| {
                let meta = envelope.meta();
                let page = PageContext {
                    cover: Some(&fixture.stream),
                    sequence: envelope.sequence(),
                    meta: Some(&meta),
                };
                D::apply(&mut state, envelope.event(), &page);
                (envelope.sequence(), state.clone())
            })
            .collect();
        for (sequence, expected) in &fixture.states {
            match states.iter().find(|(s, _)| s == sequence) {
                None => failures.push(format!("{name}: no event {sequence}")),
                Some((_, state)) if state != expected => failures.push(format!(
                    "{name}: after event {sequence}, {state:?}, not {expected:?}"
                )),
                Some(_) => {}
            }
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures)
    }
}

/// [`check_replay_fixtures`], panicking with every failure: for tests.
#[track_caller]
pub fn assert_replay_fixtures<D>(registry: &Registry, fixtures: &[ReplayFixture<'_, D::State>])
where
    D: Domain,
    D::State: PartialEq + Debug,
{
    if let Err(failures) = check_replay_fixtures::<D>(registry, fixtures) {
        panic!(
            "{} replay fixtures do not replay:\n{}",
            D::NAME,
            failures.join("\n")
        );
    }
}
