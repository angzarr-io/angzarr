//! The tests' toy domain and fakes: a **note**, written once, rewritten,
//! erased, in angzarr-embedded-macros' markup, refusing a second write and
//! anything after its erasure with its own typed rejection; and a clock and
//! a delay whose every reading, wait and draw is exact. Any marked-up domain
//! would do; this one is the smallest that refuses, and whose state a shape
//! can read.

use std::fmt;
use std::time::Duration;

use ::angzarr_embedded::markup::{self, Context, DomainMessage, Refuses};
use ::angzarr_embedded::{Any, Payload, PayloadError, Timestamp, WireError};
use angzarr_embedded_macros::{aggregate, applies, handles_cmd};
use coded_diagnostic::{code, Arg, Diagnostic};

use crate::{Clock, Delay, Jitter};

/// The note's domain.
pub const NOTE_DOMAIN: &str = "note";

// ---------------------------------------------------------------- the wire

/// The prost messages the note's types travel as.
mod wire {
    macro_rules! message {
        ($package:literal, $name:ident { $($(#[$attr:meta])* $field:ident : $ty:ty),* $(,)? }) => {
            #[derive(Clone, PartialEq, ::prost::Message)]
            pub struct $name {
                $($(#[$attr])* pub $field: $ty,)*
            }

            impl ::prost::Name for $name {
                const NAME: &'static str = stringify!($name);
                const PACKAGE: &'static str = $package;
            }
        };
    }

    message!(
        "toy.note.cmd.v1",
        Write {
            #[prost(string, tag = "1")]
            text: String,
        }
    );
    message!(
        "toy.note.cmd.v1",
        Rewrite {
            #[prost(string, tag = "1")]
            text: String,
        }
    );
    message!("toy.note.cmd.v1", Erase {});
    message!(
        "toy.note.v1",
        Written {
            #[prost(string, tag = "1")]
            text: String,
        }
    );
    message!(
        "toy.note.v1",
        Rewritten {
            #[prost(string, tag = "1")]
            text: String,
        }
    );
    message!("toy.note.v1", Erased {});
    // The note's anchor: the state it registers, and never snapshots.
    message!("toy.note.v1", NoteState {});
    // The note's typed refusal object: why, and the text it holds.
    message!(
        "toy.note.v1",
        NoteRefused {
            #[prost(string, tag = "1")]
            kind: String,
            #[prost(string, tag = "2")]
            text: String,
        }
    );
}

// ---------------------------------------------------------------- the types

/// A domain type whose fields are its wire message's, one for one.
macro_rules! domain_type {
    ($name:ident { $($field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Clone, PartialEq, Debug)]
        pub struct $name {
            $(pub $field: $ty,)*
        }

        impl DomainMessage for $name {
            type Wire = wire::$name;

            fn to_wire(&self) -> wire::$name {
                wire::$name { $($field: self.$field.clone(),)* }
            }

            fn from_wire(wire: wire::$name) -> Result<Self, WireError> {
                let _ = &wire;
                Ok($name { $($field: wire.$field,)* })
            }
        }
    };
}

domain_type!(Write { text: String });
domain_type!(Rewrite { text: String });
domain_type!(Erase {});
domain_type!(Written { text: String });
domain_type!(Rewritten { text: String });
domain_type!(Erased {});
domain_type!(NoteState {});

/// An enum of the note's types, one variant per type, with a `From` of
/// each and its `Payload`.
macro_rules! payload {
    ($enum:ident { $($name:ident),* $(,)? }) => {
        #[derive(Clone, PartialEq, Debug)]
        pub enum $enum {
            $($name($name),)*
        }

        $(impl From<$name> for $enum {
            fn from(value: $name) -> Self {
                $enum::$name(value)
            }
        })*

        impl Payload for $enum {
            fn to_any(&self) -> Any {
                match self {
                    $($enum::$name(value) => markup::pack_message(value),)*
                }
            }

            fn from_any(any: &Any) -> Result<Self, PayloadError> {
                $(if let Some(value) = markup::decode_message::<$name>(any)? {
                    return Ok($enum::$name(value));
                })*
                Err(PayloadError::UnknownType {
                    type_url: any.type_url.clone(),
                })
            }

            fn type_urls() -> Vec<String> {
                vec![$(markup::type_url_of::<$name>(),)*]
            }
        }
    };
}

payload!(NoteCommand {
    Write,
    Rewrite,
    Erase
});
payload!(NoteEvent {
    Written,
    Rewritten,
    Erased
});

// ---------------------------------------------------------------- the rejection

/// Why the note refused a command: its typed refusal object, travelling as
/// `toy.note.v1.NoteRefused`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum NoteRejection {
    /// A write to a note already written: `text`, what it holds.
    Written { text: String },
    /// A command to an erased note.
    Erased,
}

impl fmt::Display for NoteRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        coded_diagnostic::display(self, f)
    }
}

impl Diagnostic for NoteRejection {
    fn code(&self) -> &str {
        match self {
            NoteRejection::Written { .. } => code!("note-written"),
            NoteRejection::Erased => code!("note-erased"),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            NoteRejection::Written { text } => vec![("text", text.as_str().into())],
            NoteRejection::Erased => Vec::new(),
        }
    }
}

impl DomainMessage for NoteRejection {
    type Wire = wire::NoteRefused;

    fn to_wire(&self) -> wire::NoteRefused {
        let text = match self {
            NoteRejection::Written { text } => text.clone(),
            NoteRejection::Erased => String::new(),
        };
        wire::NoteRefused {
            kind: self.code().to_string(),
            text,
        }
    }

    fn from_wire(wire: wire::NoteRefused) -> Result<Self, WireError> {
        match wire.kind.as_str() {
            code!("note-written") => Ok(NoteRejection::Written { text: wire.text }),
            code!("note-erased") => Ok(NoteRejection::Erased),
            _ => Err(WireError::Missing {
                message: "toy.note.v1.NoteRefused",
                field: "kind",
            }),
        }
    }
}

// ---------------------------------------------------------------- the note

/// One note, folded from its stream.
#[derive(Clone, Debug, Default)]
pub struct Note {
    pub text: String,
    pub erased: bool,
}

impl Note {
    /// Refused once erased.
    fn open(&self) -> Result<(), NoteRejection> {
        match self.erased {
            true => Err(NoteRejection::Erased),
            false => Ok(()),
        }
    }
}

/// Names its anchor and takes no snapshots: every load replays.
impl markup::Snapshots for Note {
    type Anchor = NoteState;
}

#[aggregate(domain = "note", command = NoteCommand, event = NoteEvent)]
impl Note {
    /// Write the note: refused on a stream with history.
    #[handles_cmd(Write, refuses(NoteRejection))]
    fn write(&self, cmd: Write, ctx: &Context) -> Result<Written, Refuses<Self, Write>> {
        match ctx.command.had_prior_events {
            true => Err(NoteRejection::Written {
                text: self.text.clone(),
            }
            .into()),
            false => Ok(Written { text: cmd.text }),
        }
    }

    #[handles_cmd(Rewrite, refuses(NoteRejection))]
    fn rewrite(&self, cmd: Rewrite, ctx: &Context) -> Result<Rewritten, Refuses<Self, Rewrite>> {
        let _ = ctx;
        self.open()?;
        Ok(Rewritten { text: cmd.text })
    }

    #[handles_cmd(Erase, refuses(NoteRejection))]
    fn erase(&self, cmd: Erase, ctx: &Context) -> Result<Erased, Refuses<Self, Erase>> {
        let _ = (cmd, ctx);
        self.open()?;
        Ok(Erased {})
    }

    #[applies(Written)]
    fn written(&mut self, event: Written) {
        self.text = event.text;
    }

    #[applies(Rewritten)]
    fn rewritten(&mut self, event: Rewritten) {
        self.text = event.text;
    }

    #[applies(Erased)]
    fn erased(&mut self, event: Erased) {
        let _ = event;
        self.erased = true;
    }
}

// ---------------------------------------------------------------- the fakes

/// A clock one second on at each reading (from second 1_700_000_001).
#[derive(Default)]
pub struct FakeClock {
    pub seconds: i64,
}

impl Clock for FakeClock {
    fn now(&mut self) -> Timestamp {
        self.seconds += 1;
        Timestamp {
            seconds: 1_700_000_000 + self.seconds,
            nanos: 0,
        }
    }
}

/// Waits recorded, never waited; jitter is a fixed fraction (in
/// thousandths, the whole ceiling by default) of the ceiling, or a fixed
/// duration when `over` is set. Each ceiling asked for is kept.
pub struct FakeDelay {
    pub slept: Vec<Duration>,
    pub ceilings: Vec<Duration>,
    pub permille: u32,
    pub over: Option<Duration>,
}

impl Default for FakeDelay {
    fn default() -> Self {
        FakeDelay {
            slept: Vec::new(),
            ceilings: Vec::new(),
            permille: 1000,
            over: None,
        }
    }
}

impl Delay for FakeDelay {
    async fn sleep(&mut self, duration: Duration) {
        self.slept.push(duration);
    }
}

impl Jitter for FakeDelay {
    fn up_to(&mut self, ceiling: Duration) -> Duration {
        self.ceilings.push(ceiling);
        self.over.unwrap_or(ceiling * self.permille / 1000)
    }
}
