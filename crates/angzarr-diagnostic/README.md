# angzarr-diagnostic

Localizable errors. Zero dependencies and nothing specific to any program.

Every error in angzarr's shared crates, and in the runtimes and apps built on
them, implements `Diagnostic`:

```rust
pub trait Diagnostic: fmt::Debug + fmt::Display {
    fn code(&self) -> &str;              // the Fluent message id
    fn args(&self) -> Vec<(&str, Arg)>;  // the message's named args
}

pub enum Arg { Int(i64), Uint(u64), Str(String), Nested(Box<dyn Diagnostic>) }
```

- **The code** is stable, kebab-case and starts with the crate's name:
  `angzarr-yaml-too-new`. A code is never reused for another meaning.
- **The args** are data: numbers, ids, paths, the text that was read.
  Never English. A wrapped cause is `Arg::Nested`.
- **`Display`** is the code, then ` name=value` for each arg, a nested
  cause as `name=<its code>` and `name.<arg>=value` (angzarr decision
  14.37): never English. A value that is empty or holds whitespace, `=`,
  `"` or `\` is double-quoted, its `"` and `\` backslash-escaped, so the
  line parses back (`quote`). `display(d, f)` writes it.

Each crate ships an en-US Fluent catalog,
`crates/<crate>/locales/en-US/<crate>.ftl`, with a message for every code,
and exports it as `EN_US`. A library renders nothing: whoever shows the error
looks the code up in its catalogs and passes the args by name, rendering a
nested cause first. `catalog::render(ftl, d, f)` does that with no Fluent
library, for a catalog whose messages are one line of text and `{ $arg }`
placeables; any other message, or a code the catalog lacks, falls back to
the code and args (`display`).

A program's own errors implement `Diagnostic` too, with codes starting with
the program's name. Where a library's error holds its caller's error (a
migration step's, a content decoder's) it keeps it as a `Report`: the code
and args captured, so the holder stays `Clone` and `Eq`.
`Report::new(code).arg(name, value)` raises one without an error type of its
own.

Moved from angzarr-embedded's `angzarr-embedded-diagnostic` (architecture
fixes B1); the API is unchanged.
