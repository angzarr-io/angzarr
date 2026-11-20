//! The child conformance probe: one child launched as the host launches
//! it, taken through the child protocol (angzarr-project's
//! `docs/design/child-protocol.md`) a step at a time, each reported as
//! passed or as the keyed failure the host would have refused it with. What
//! a client library for a new language runs against its own child shim,
//! without an application around it.
//!
//! The steps, in order ([`Step`]): launch it; wait for its gRPC health to
//! report SERVING on the socket the host chose, within the start limit
//! (there is no handshake line: decision 14.80); describe it, its
//! descriptor of
//! the kind its entry says and valid; hand it one input ([`Input`]: a
//! command for a domain, a book of events for a policy or a projector),
//! which it must answer; and stop it, after which it must have exited by
//! itself, with status 0, on its stdin's end. A step that fails ends the
//! probe: the child is stopped, and every step after it is not run. An
//! upcaster sidecar is not described and takes no input: those two steps
//! are not run for it.

use std::time::Duration;

use angzarr_diagnostic::Report;
use angzarr_embedded::KernelError;
use angzarr_proto::v1 as pb;
use angzarr_router::hpb;
use angzarr_router::router::{CommandHandler, PolicyComponent, ProjectorHandler};

use crate::config::{ComponentConfig, ComponentKindConfig, Languages, Placement, Supervision};
use crate::error::HostError;
use crate::host::{socket_dir, ChildExit, Hooks, Supervisor, Up};
use crate::remote::{Link, RemoteCommandHandler, RemotePolicyHandler, RemoteProjectorHandler};

/// One step of the child protocol, as the probe takes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    /// Its launch command found, and started.
    Launch,
    /// Its gRPC health watch reporting SERVING on the socket the host
    /// chose, within the start limit (`EMBEDDED_REASON_CHILD_NOT_SERVING`).
    Serving,
    /// `ComponentService.Describe` answered with a valid descriptor of
    /// its entry's kind.
    Describe,
    /// One input handled: answered, not refused and not unreachable.
    Handle,
    /// Stopped by its stdin's end: exited by itself, with status 0.
    Shutdown,
}

/// How a step went.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Passed,
    /// Failed: why, keyed as the host would refuse it (a [`HostError`], or
    /// the kernel's error for the input's answer).
    Failed(Report),
    /// Not run: a step before it failed, or it does not apply to the
    /// child's kind.
    NotRun,
}

/// What the probe hands the child to handle: a command for a domain (an
/// aggregate or a process manager), as its `HandleRequest`; a book of
/// events for a policy (its first page, the trigger) or a projector.
#[derive(Clone, Debug)]
pub enum Input {
    Command(Box<hpb::HandleRequest>),
    Events(pb::EventBook),
}

/// The root of the stream a probe's [`Input::command`] and
/// [`Input::event`] are on.
pub const PROBE_ROOT: [u8; 16] = [7; 16];

impl Input {
    /// A command to `domain`, on [`PROBE_ROOT`]: one page, its payload of
    /// `type_url`, `payload` its encoded message (empty: every field its
    /// default).
    pub fn command(domain: &str, type_url: &str, payload: Vec<u8>) -> Input {
        Input::Command(Box::new(hpb::HandleRequest {
            input: Some(pb::CommandBook {
                cover: Some(probe_cover(domain)),
                page: Some(pb::CommandPage {
                    body: Some(pb::command_page::Body::Command(pb::Command {
                        payload: Some(packed(type_url, payload)),
                        handling: None,
                    })),
                    ..pb::CommandPage::default()
                }),
            }),
            history: Some(pb::EventBook {
                cover: Some(probe_cover(domain)),
                ..pb::EventBook::default()
            }),
            ..hpb::HandleRequest::default()
        }))
    }

    /// One event of `domain`, on [`PROBE_ROOT`], at sequence 0: its payload
    /// of `type_url`, `payload` its encoded message.
    pub fn event(domain: &str, type_url: &str, payload: Vec<u8>) -> Input {
        Input::Events(pb::EventBook {
            cover: Some(probe_cover(domain)),
            pages: vec![pb::EventPage {
                sequence: 0,
                body: Some(pb::event_page::Body::Event(packed(type_url, payload))),
                ..pb::EventPage::default()
            }],
            next_sequence: 1,
            snapshot: None,
        })
    }
}

fn probe_cover(domain: &str) -> pb::Cover {
    pb::Cover {
        domain: domain.to_string(),
        root: Some(pb::Uuid::from_bytes(PROBE_ROOT)),
        ..pb::Cover::default()
    }
}

fn packed(type_url: &str, value: Vec<u8>) -> prost_types::Any {
    prost_types::Any {
        type_url: type_url.to_string(),
        value,
    }
}

/// What the probe found: every step, in order, with how it went.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Probed {
    pub steps: Vec<(Step, Outcome)>,
}

impl Probed {
    /// Whether every step that applies passed: the shutdown did, which
    /// runs only when every step before it passed.
    pub fn passed(&self) -> bool {
        (self.steps.iter())
            .any(|(step, outcome)| *step == Step::Shutdown && *outcome == Outcome::Passed)
    }
}

/// The steps, in order.
const STEPS: [Step; 5] = [
    Step::Launch,
    Step::Serving,
    Step::Describe,
    Step::Handle,
    Step::Shutdown,
];

/// Probe `component`, launched as its language in `languages` launches it
/// (its `launch`, else its language's default and its `entry`), under
/// `supervision`'s time limits, handing it `input`. It blocks until the
/// child has ended, on a runtime of its own.
pub fn probe(
    component: &ComponentConfig,
    languages: &Languages,
    supervision: &Supervision,
    input: Input,
) -> Probed {
    let mut steps = Vec::new();
    let mut ran = |step: Step, outcome: Result<(), Report>| {
        steps.push((step, outcome.err().map_or(Outcome::Passed, Outcome::Failed)));
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            let refused = HostError::Runtime {
                kind: format!("{:?}", e.kind()),
            };
            ran(Step::Launch, Err(Report::of(&refused)));
            return not_run_after(steps);
        }
    };
    let supervisor = match supervisor(component, languages, supervision) {
        Ok(supervisor) => supervisor,
        Err(refused) => {
            ran(Step::Launch, Err(Report::of(&refused)));
            return not_run_after(steps);
        }
    };
    runtime.block_on(async {
        let mut up = match supervisor.come_up().await {
            Ok(up) => up,
            Err(refused) => {
                let failed = failed_at(&refused);
                for step in STEPS.into_iter().take_while(|step| *step != failed) {
                    ran(step, Ok(()));
                }
                ran(failed, Err(Report::of(&refused)));
                return;
            }
        };
        for step in [Step::Launch, Step::Serving] {
            ran(step, Ok(()));
        }
        let call_timeout = Duration::from_millis(supervision.call_timeout_ms);
        let handled = match component.kind {
            ComponentKindConfig::Upcaster => None,
            kind => {
                ran(Step::Describe, Ok(()));
                Some(handle(kind, &component.name, &up, call_timeout, input).await)
            }
        };
        match handled {
            Some(Err(failure)) => {
                ran(Step::Handle, Err(failure));
                supervisor.stop_child(&mut up).await;
                return;
            }
            Some(Ok(())) => ran(Step::Handle, Ok(())),
            None => {}
        }
        let exit = supervisor.stop_child(&mut up).await;
        drop(up);
        ran(
            Step::Shutdown,
            match exit {
                ChildExit::Exited(Some(0)) => Ok(()),
                ChildExit::Exited(Some(exit_code)) => Err(Report::of(&HostError::ExitStatus {
                    component: component.name.clone(),
                    exit_code,
                })),
                exit => Err(Report::of(&HostError::NotExited {
                    component: component.name.clone(),
                    ended: exit.ended().to_string(),
                })),
            },
        );
    });
    not_run_after(steps)
}

/// `steps`, with every step not among them not run, in order.
fn not_run_after(mut steps: Vec<(Step, Outcome)>) -> Probed {
    let all = STEPS.into_iter().map(|step| {
        let found = steps.iter().position(|(ran, _)| *ran == step);
        found.map_or((step, Outcome::NotRun), |at| steps[at].clone())
    });
    steps = all.collect();
    Probed { steps }
}

/// The step a refusal of [`Supervisor::come_up`] fails.
fn failed_at(refused: &HostError) -> Step {
    match refused {
        HostError::NotServing { .. } => Step::Serving,
        HostError::Describe { .. }
        | HostError::KindMismatch { .. }
        | HostError::Descriptor { .. } => Step::Describe,
        _ => Step::Launch,
    }
}

/// A supervisor of `component` that is never asked to restart it.
fn supervisor(
    component: &ComponentConfig,
    languages: &Languages,
    supervision: &Supervision,
) -> Result<Supervisor, HostError> {
    let language =
        languages
            .get(&component.language)
            .ok_or_else(|| HostError::UnknownLanguage {
                key: "language".to_string(),
                language: component.language.clone(),
                loaded: languages.names().collect::<Vec<_>>().join(","),
            })?;
    let Placement::Remote(launch) = language.placement(component)? else {
        return Err(HostError::NoLaunch {
            domain: component.name.clone(),
            language: component.language.clone(),
        });
    };
    let socket_dir = socket_dir(
        supervision.socket_dir.as_deref(),
        std::env::var_os("XDG_RUNTIME_DIR").map(std::path::PathBuf::from),
    )?;
    Ok(Supervisor {
        domain: component.name.clone(),
        kind: component.kind,
        launch,
        supervision: supervision.clone(),
        socket_dir,
        link: Default::default(),
        hooks: Hooks::default(),
    })
}

/// `input` handled by the child `up`, of `kind`, through the host's remote
/// handler of that kind: answered, or why not (the router's error, as the
/// kernel carries it). An input of another kind than the child's is not
/// handed ([`HostError::ProbeInput`]).
async fn handle(
    kind: ComponentKindConfig,
    name: &str,
    up: &Up,
    call_timeout: Duration,
    input: Input,
) -> Result<(), Report> {
    let link = std::sync::Arc::new(Link::default());
    *link
        .channel
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Ok(up.channel.clone());
    let (name, descriptor) = (name.to_string(), up.descriptor.clone());
    let answered = match (kind, input) {
        (ComponentKindConfig::Domain, Input::Command(command)) => {
            RemoteCommandHandler::new(name, descriptor, link, call_timeout)
                .dispatch(&command)
                .await
                .map(drop)
        }
        (ComponentKindConfig::Policy, Input::Events(book)) => {
            RemotePolicyHandler::new(name, descriptor, link, call_timeout)
                .react(&hpb::ReactRequest {
                    trigger: Some(book),
                })
                .await
                .map(drop)
        }
        (ComponentKindConfig::Projector, Input::Events(book)) => {
            RemoteProjectorHandler::new(name, descriptor, link, call_timeout)
                .dispatch(&hpb::ProjectRequest { events: Some(book) })
                .await
                .map(drop)
        }
        (kind, _) => {
            return Err(Report::of(&HostError::ProbeInput {
                component: name,
                kind: kind.as_str().to_string(),
            }))
        }
    };
    answered.map_err(|coded| Report::of(&KernelError::Router(coded)))
}

#[cfg(test)]
#[path = "probe.test.rs"]
mod tests;
