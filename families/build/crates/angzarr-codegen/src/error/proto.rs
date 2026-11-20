//! The descriptor set as protobuf: bytes this generator cannot read, or
//! types and names the generated domain layer cannot map. Each names the
//! proto element to change.

use std::fmt;

use angzarr_diagnostic::{Arg, Diagnostic, code};

use super::{text, texts};

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ProtoError {
    /// The bytes are not a descriptor set protoc could have written.
    Descriptors { detail: String },
    /// Two of a package's domain items taking one name: types (a nested
    /// one named after its enclosing messages), a oneof's choice
    /// (`<Message><Oneof>`, `second` the oneof's full name) or an id
    /// newtype (`second` the field declaring it). Nothing is renamed
    /// silently.
    NameTaken {
        name: String,
        first: String,
        second: String,
    },
    /// A field declaring `json_flatten` that is not a singular message
    /// field of a type written as an object.
    JsonFlatten { message: String, field: String },
    /// A well-known type the domain layer does not map: every one but
    /// `google.protobuf.Timestamp` and `google.protobuf.Duration`.
    WellKnownType {
        message: String,
        field: String,
        type_name: String,
    },
    /// An enum whose zero value is not `*_UNSPECIFIED`: zero is the wire's
    /// absence, never a domain value, so it must say so.
    EnumZero { enum_name: String, value: String },
    /// A message or enum named as one of the names the generated domain
    /// layer gives the runtime's types (`Context`, `PolicyContext`,
    /// `CommandContext`, `Refused`, `Bug`, `Failed`, `BusinessStatus`,
    /// `RetryableStatus`): the generated code would
    /// collide, and nothing is renamed silently.
    ReservedName { message: String, name: String },
    /// A domain type taking the name the domain layer gives the choice of a
    /// command refusing several types (`<Command>Refuses`).
    RefusalChoiceName {
        message: String,
        name: String,
        command: String,
    },
    /// A domain type `message` takes `name`, which the generated domain
    /// layer gives `component`'s interface or its facts' choice.
    InterfaceName {
        message: String,
        name: String,
        component: String,
    },
}

impl fmt::Display for ProtoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for ProtoError {}

impl Diagnostic for ProtoError {
    fn code(&self) -> &'static str {
        match self {
            ProtoError::Descriptors { .. } => code!("descriptors"),
            ProtoError::NameTaken { .. } => code!("name-taken"),
            ProtoError::JsonFlatten { .. } => code!("json-flatten"),
            ProtoError::WellKnownType { .. } => code!("well-known-type"),
            ProtoError::EnumZero { .. } => code!("enum-zero"),
            ProtoError::ReservedName { .. } => code!("reserved-name"),
            ProtoError::RefusalChoiceName { .. } => code!("refusal-choice-name"),
            ProtoError::InterfaceName { .. } => code!("interface-name"),
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        use ProtoError as E;
        match self {
            E::Descriptors { detail } => texts![detail],
            E::NameTaken {
                name,
                first,
                second,
            } => texts![name, first, second],
            E::JsonFlatten { message, field } => texts![message, field],
            E::WellKnownType {
                message,
                field,
                type_name,
            } => texts![message, field, type_name],
            E::EnumZero { enum_name, value } => texts![enum_name, value],
            E::ReservedName { message, name } => texts![message, name],
            E::RefusalChoiceName {
                message,
                name,
                command,
            } => texts![message, name, command],
            E::InterfaceName {
                message,
                name,
                component,
            } => texts![message, name, component],
        }
    }
}

/// One value of every [`ProtoError`].
pub(super) fn samples() -> Vec<ProtoError> {
    use ProtoError as E;
    vec![
        E::Descriptors {
            detail: text("detail"),
        },
        E::NameTaken {
            name: text("name"),
            first: text("first"),
            second: text("second"),
        },
        E::JsonFlatten {
            message: text("message"),
            field: text("field"),
        },
        E::WellKnownType {
            message: text("message"),
            field: text("field"),
            type_name: text("type_name"),
        },
        E::EnumZero {
            enum_name: text("enum_name"),
            value: text("value"),
        },
        E::ReservedName {
            message: text("message"),
            name: text("name"),
        },
        E::RefusalChoiceName {
            message: text("message"),
            name: text("name"),
            command: text("command"),
        },
        E::InterfaceName {
            message: text("message"),
            name: text("name"),
            component: text("component"),
        },
    ]
}
