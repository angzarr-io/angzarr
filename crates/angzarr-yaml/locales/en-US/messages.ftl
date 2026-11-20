# Messages for angzarr-yaml's errors. Each id is an error's code without
# the crate's name, which the build prefixes (angzarr_diagnostic::build);
# the variables are its args. Messages are lowercase fragments, like the English
# Display, so they read well inside another message. A literal brace is
# written {"{"} or {"}"}.

## Parsing. Every parse error has $line, 1-based (0 for the end of input).

lone-carriage-return = line { $line }: a carriage return without a line feed: lines end in \n or \r\n
tab-indent = line { $line }: tabs can't indent
non-ascii-indent = line { $line }: indentation is ASCII spaces only, not { $character }
document-marker = line { $line }: one document per file: no { $marker }
anchor = line { $line }: anchors (&) aren't supported
alias = line { $line }: aliases (*) aren't supported
tag = line { $line }: tags (!) aren't supported
block-scalar = line { $line }: a block scalar is read only after a key or a dash, with | or > then -, + or nothing (no indentation indicator)
unexpected-indent = line { $line }: unexpected indentation
expected-key-value = line { $line }: expected `key: value`, found "{ $found }"
key-not-scalar = line { $line }: keys must be scalars
duplicate-key = line { $line }: "{ $key }" twice
unexpected = line { $line }: unexpected "{ $found }"
expected-seq-separator = line { $line }: expected , or ] in a flow sequence
expected-colon = line { $line }: expected : in a flow mapping
expected-map-separator = line { $line }: expected , or {"}"} in a flow mapping
bad-unicode-escape = line { $line }: bad escape { $escape }: \u takes 4 hex digits and \U 8, naming a Unicode scalar value; surrogates are refused (write \U0001F5E1, not 🗡)
unknown-escape = line { $line }: unknown escape { $escape }
unterminated-quote = line { $line }: unterminated quote

## Versioned documents. $kind is the document kind the caller named; $source
## is where the document came from (a path, say).

load-failed = { $kind } document: { $problem }
load-failed-at = { $kind } document { $source }: { $problem }

root-not-a-mapping = the root must be a mapping, stamped with its format version
version-empty = `{ $key }` must be a non-negative integer, not an empty value
version-sequence = `{ $key }` must be a non-negative integer, not a sequence
version-mapping = `{ $key }` must be a non-negative integer, not a mapping
version-not-an-integer = `{ $key }` must be a non-negative integer, not "{ $text }"
version-too-large = `{ $key }` must be a non-negative integer, not "{ $text }" (too large)
legacy-and-stamp = both `{ $key }` and the old `{ $legacy }` it replaces are set: keep `{ $key }` alone
too-new = version { $found } was written by a newer build; this build reads up to version { $current }
too-old = version { $found } is older than this build migrates; the oldest it reads is version { $oldest }
step-failed = migrating version { $from } to { $to }: { $error }

## Migration steps. $from and $to are key paths, written dotted.

rename-not-a-mapping = rename "{ $from }": not a mapping
rename-target-exists = rename "{ $from }" to "{ $to }": "{ $to }" is already there
move-empty-path = move { $from } to { $to }: empty key path
move-not-a-mapping = move { $from } to { $to }: not a mapping
move-through-not-a-mapping = move { $from } to { $to }: "{ $key }": not a mapping
move-target-exists = move { $from } to { $to }: "{ $key }" is already there
default-not-a-mapping = default "{ $key }": not a mapping
remove-not-a-mapping = remove "{ $key }": not a mapping
map-not-a-mapping = map over "{ $key }": not a mapping
map-not-a-sequence = map over "{ $key }": not a sequence
map-item = { $key }[{ $index }]: { $error }
step-root-not-a-mapping = the step left the root not a mapping

## Migration chains.

chain-duplicate = { $kind } migrations: two steps from version { $from }
chain-beyond-current = { $kind } migrations: a step from version { $from }, but the current version is { $current }
chain-gap = { $kind } migrations: no step from version { $from } to { $to }

## Decoding a document into a type. $key_path is where, keys dotted and
## items bracketed (empty for the root); $expected and $found are tokens
## (string, u16, mapping, null, ...) or the text that was read.

decode-type = `{ $key_path }`: expected a { $expected }, found a { $found }
decode-scalar = `{ $key_path }`: "{ $found }" is not a { $expected }
decode-unknown-key = `{ $key_path }`: unknown key "{ $found }" (the keys are: { $expected })
decode-missing-key = `{ $key_path }`: the key "{ $expected }" is { $found ->
    [absent] missing
   *[other] { $found }
}
decode-unknown-variant = `{ $key_path }`: unknown variant "{ $found }" (the variants are: { $expected })
decode-length = `{ $key_path }`: expected { $expected } items, found { $found }
decode-invalid = `{ $key_path }`: "{ $found }" is not a valid { $expected }: { $ext_err_txt }

## Writing upgrades back (--write-upgrades). $path is the file; $cause is
## the file system's own error, rendered by its own message.

write-back-read = { $path } could not be read, so it was not upgraded: { $cause }
write-back-backup = { $path } could not be written, so the file it backs up was not upgraded: { $cause }
write-back-write = { $path } could not be replaced with its upgrade: { $cause }

## Editing a document in place. $path is the key path, written dotted, or
## "the root".

edit-not-found = { $path }: not found
edit-not-a-mapping = { $path }: not a mapping
edit-not-a-sequence = { $path }: not a sequence
edit-inside-flow = { $path }: inside a flow collection, which is edited only as a whole
edit-item-first-key = { $path }: the key on a sequence item's dash line, which is edited only with the whole item
edit-no-such-item = { $path }: no item { $index }: the sequence has { $len ->
    [one] { $len } item
   *[other] { $len } items
}
edit-unfaithful = { $path }: the edit would not read back as written; nothing was changed
edit-key-exists = { $path }: cannot be renamed to "{ $key }", which is already there
