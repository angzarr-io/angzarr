# Messages for angzarr-yaml's errors. Each id is an error's code; the
# variables are its args. Messages are lowercase fragments, like the English
# Display, so they read well inside another message. A literal brace is
# written {"{"} or {"}"}.

## Parsing. Every parse error has $line, 1-based (0 for the end of input).

angzarr-yaml-lone-carriage-return = line { $line }: a carriage return without a line feed: lines end in \n or \r\n
angzarr-yaml-tab-indent = line { $line }: tabs can't indent
angzarr-yaml-non-ascii-indent = line { $line }: indentation is ASCII spaces only, not { $character }
angzarr-yaml-document-marker = line { $line }: one document per file: no { $marker }
angzarr-yaml-anchor = line { $line }: anchors (&) aren't supported
angzarr-yaml-alias = line { $line }: aliases (*) aren't supported
angzarr-yaml-tag = line { $line }: tags (!) aren't supported
angzarr-yaml-block-scalar = line { $line }: block scalars (| and >) aren't supported
angzarr-yaml-unexpected-indent = line { $line }: unexpected indentation
angzarr-yaml-expected-key-value = line { $line }: expected `key: value`, found "{ $found }"
angzarr-yaml-key-not-scalar = line { $line }: keys must be scalars
angzarr-yaml-duplicate-key = line { $line }: "{ $key }" twice
angzarr-yaml-unexpected = line { $line }: unexpected "{ $found }"
angzarr-yaml-expected-seq-separator = line { $line }: expected , or ] in a flow sequence
angzarr-yaml-expected-colon = line { $line }: expected : in a flow mapping
angzarr-yaml-expected-map-separator = line { $line }: expected , or {"}"} in a flow mapping
angzarr-yaml-bad-unicode-escape = line { $line }: bad escape { $escape }: \u takes 4 hex digits and \U 8, naming a Unicode scalar value; surrogates are refused (write \U0001F5E1, not 🗡)
angzarr-yaml-unknown-escape = line { $line }: unknown escape { $escape }
angzarr-yaml-unterminated-quote = line { $line }: unterminated quote

## Versioned documents. $kind is the document kind the caller named; $source
## is where the document came from (a path, say).

angzarr-yaml-load-failed = { $kind } document: { $problem }
angzarr-yaml-load-failed-at = { $kind } document { $source }: { $problem }

angzarr-yaml-root-not-a-mapping = the root must be a mapping with a `version`
angzarr-yaml-version-empty = `version` must be a non-negative integer, not an empty value
angzarr-yaml-version-sequence = `version` must be a non-negative integer, not a sequence
angzarr-yaml-version-mapping = `version` must be a non-negative integer, not a mapping
angzarr-yaml-version-not-an-integer = `version` must be a non-negative integer, not "{ $text }"
angzarr-yaml-version-too-large = `version` must be a non-negative integer, not "{ $text }" (too large)
angzarr-yaml-too-new = version { $found } was written by a newer build; this build reads up to version { $current }
angzarr-yaml-too-old = version { $found } is older than this build migrates; the oldest it reads is version { $oldest }
angzarr-yaml-step-failed = migrating version { $from } to { $to }: { $error }

## Migration steps. $from and $to are key paths, written dotted.

angzarr-yaml-rename-not-a-mapping = rename "{ $from }": not a mapping
angzarr-yaml-rename-target-exists = rename "{ $from }" to "{ $to }": "{ $to }" is already there
angzarr-yaml-move-empty-path = move { $from } to { $to }: empty key path
angzarr-yaml-move-not-a-mapping = move { $from } to { $to }: not a mapping
angzarr-yaml-move-through-not-a-mapping = move { $from } to { $to }: "{ $key }": not a mapping
angzarr-yaml-move-target-exists = move { $from } to { $to }: "{ $key }" is already there
angzarr-yaml-default-not-a-mapping = default "{ $key }": not a mapping
angzarr-yaml-remove-not-a-mapping = remove "{ $key }": not a mapping
angzarr-yaml-map-not-a-mapping = map over "{ $key }": not a mapping
angzarr-yaml-map-not-a-sequence = map over "{ $key }": not a sequence
angzarr-yaml-map-item = { $key }[{ $index }]: { $error }
angzarr-yaml-step-root-not-a-mapping = the step left the root not a mapping

## Migration chains.

angzarr-yaml-chain-duplicate = { $kind } migrations: two steps from version { $from }
angzarr-yaml-chain-beyond-current = { $kind } migrations: a step from version { $from }, but the current version is { $current }
angzarr-yaml-chain-gap = { $kind } migrations: no step from version { $from } to { $to }

## Editing a document in place. $path is the key path, written dotted, or
## "the root".

angzarr-yaml-edit-not-found = { $path }: not found
angzarr-yaml-edit-not-a-mapping = { $path }: not a mapping
angzarr-yaml-edit-not-a-sequence = { $path }: not a sequence
angzarr-yaml-edit-inside-flow = { $path }: inside a flow collection, which is edited only as a whole
angzarr-yaml-edit-item-first-key = { $path }: the key on a sequence item's dash line, which is edited only with the whole item
angzarr-yaml-edit-no-such-item = { $path }: no item { $index }: the sequence has { $len ->
    [one] { $len } item
   *[other] { $len } items
}
angzarr-yaml-edit-unfaithful = { $path }: the edit would not read back as written; nothing was changed
