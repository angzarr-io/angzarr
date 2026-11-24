# Messages for the angzarr-embedded crate's errors. Each id is an error's
# code without the crate's name, which the build prefixes
# (coded_diagnostic::build); the variables are its args. Messages are
# lowercase fragments, like the English Display, so a nested cause reads
# well inside another message. A condition the spec keys is coded under its
# key, whose English is the spec's catalog (angzarr_proto::REASONS_EN_US;
# decision 14.60), never here.

## The audited kernel, angzarr_embedded's crate root. $stream names a stream as domain/root,
## with @edition outside the main timeline.

malformed-no-cover = the book has no cover
malformed-unknown-domain = no domain "{ $domain }" is registered
malformed-no-root = the cover has no root
malformed-bad-root = the root is { $len ->
        [one] one byte
       *[other] { $len } bytes
    }, not a UUID's 16
malformed-empty-edition = the edition is set but empty: the main timeline is the unset edition
malformed-no-page = the book has no page
malformed-no-body = the page has neither a command nor a fact
malformed-no-payload = the command or fact has no payload
malformed-bad-command-id = the command id is { $len ->
        [one] one byte
       *[other] { $len } bytes
    }, not a UUID's 16
malformed-bad-correlation-id = the correlation id is { $len ->
        [one] one byte
       *[other] { $len } bytes
    }, not a UUID's 16
malformed-bad-trigger = the policy command's trigger cannot name a command id: { $reason ->
        [bad-root] its root is not a UUID's 16 bytes
        [missing-trigger] it needs a stream with a root
        [empty-edition] its stream's edition is set but empty
        [empty-component] the reacting component has no name
       *[other] { $reason }
    }
malformed-fact-without-origin = a fact without its origin
malformed-empty-external-id = the fact's external id is empty: it is the fact's identity
malformed-fact-command-id-not-derived = the fact's command id is not the one derived from its external id

projection-failed = the command committed, then { $failures ->
    [one] projector { $projector } failed: { $error }
   *[other] { $failures } projectors failed, the first { $projector }: { $error }
}
downstream = a policy command down the chain failed, at depth { $depth }: { $error }
sync-deadline-passed = the projector was still running when the call's deadline of { $deadline_ms } ms passed
policy-output-no-body = a policy's output holds neither a command nor a fact
unknown-type = stream { $stream }'s domain does not decode the type { $type_url }
invalid-payload = a { $type_url } for stream { $stream } is not a valid value: { $reason }
upcast = upcasting a payload for stream { $stream } failed: { $error }
wrong-domain = stream { $stream } was given to the { $expected } domain's code
before-snapshot = stream { $stream } holds its events from { $first } on only, after its snapshot
unregistered-domain = a stored { $domain } stream was read, but no domain "{ $domain }" is registered
replay-out-of-sequence = page { $index } of the log claims sequence { $found }, not the one due
replay-recorded-backwards = event { $sequence } was recorded before the event before it
replay-incomplete-meta = event { $sequence }'s metadata lacks a well-formed { $field }
replay-wrong-domain = a { $found } log was given to the { $expected } domain's code

unreadable = event { $sequence } of stream { $stream } cannot be replayed: { $reason }
predecessor-missing = event { $sequence } of stream { $stream } waits on event { $missing }, which no source has

payload-unknown-type = no decoder for the type { $type_url }
payload-decode = a payload of type { $type_url } does not decode
not-snapshotted = a snapshot of { $anchor } was handed to a state that takes none, so it cannot be restored
not-its-stream = a command whose { $domain } key is { $key } was sent to another stream: its stream is the one that key names
payload-invalid = a payload of type { $type_url } is not a valid value: { $reason }
wire-missing = { $message }.{ $field } is required and absent
wire-unknown-enum = { $message }.{ $field } holds { $value }, which no variant defines
upcast-unavailable = the upcaster sidecar is not serving ({ $cause }): retry
upcast-rule = the upcast rule from { $source } failed: { $error }
upcast-answer-keys = the upcaster answered other keys than the payloads asked: each page's sequence once, or one unsequenced payload for a lone one
upcaster-duplicate = domain { $domain }'s upcaster has two typed rules from { $source }: the second never runs
upcaster-out-of-order = domain { $domain }'s upcaster registers the rule from { $source } before the rule from { $produced_by }, which produces it
upcaster-dead-end = domain { $domain }'s upcaster takes { $source } to { $ends_at }, which the domain does not decode


## A process manager's compensation (markup::Compensation, angzarr decision 14.74).

undo-targets-command = the undo of step "{ $step }" becomes { $command }, a command its destination may refuse: an undo must become a fact
