# Messages for angzarr-store-transfer's errors. Each id is an error's code
# without the crate's name, which the build prefixes
# (angzarr_diagnostic::build); the variables are its args. A row is named by
# $kind (page, snapshot, position, dead-letter), $stream
# (domain:edition:root, the edition empty for main) and $place (a page's
# sequence, a letter's place, a position's handler).

## Export: refused, nothing written.

store = the store failed: { $cause }
ledger-not-the-builds = the store's migrations ({ $applied }) are not this build's ({ $build }): export it with the build that wrote it
pending-outbox = the outbox holds { $entries } pending entries: let the relay deliver them, then export
undecodable = the { $kind } at { $stream } { $place } does not decode as { $message }
reencoding-differs = the { $kind } at { $stream } { $place } does not re-encode to its stored bytes as { $message }: it holds a field this build does not know
unresolved-type = the { $kind } at { $stream } { $place } holds a payload of type { $type_url }, which no descriptor names
unknown-fields = the { $kind } at { $stream } { $place } holds fields { $message } does not know, at { $path }
unwritable = the { $kind } at { $stream } { $place } could not be written as proto3 JSON ({ $message })
missing-descriptor = the descriptors lack { $message }, which an export writes
descriptor-set = descriptor set { $index } does not decode, or does not join the others
write = the export could not be written ({ $kind })
