# Remediation status

X-### | status | commit | note
---|---|---|---
X-008 | fixed | angzarr-project b993c65, core 1929e9bd | skip_handler replaces route_to_handler
X-007 | fixed | angzarr-project b51ef55 | just proto-lint/proto-breaking (buf 1.47.2 container, WIRE_JSON) + check-feature-ids; .github/workflows/contracts.yml
X-151 | fixed (framework tiers) | angzarr-project b51ef55 | 211 framework scenarios tagged C-0191..C-0401; example/ deferred to blackjack
X-016 | fixed (spec) | angzarr-project 8f7a3e2 | merge_strategy.feature rewritten: field-overlap COMMUTATIVE, STRICT, AGGREGATE_HANDLES, MANUAL->DLQ; core must align statuses
X-017 | fixed (spec) | angzarr-project 97a2cc5 | sagas/PMs emit angzarr_deferred; wire_parity hashes replaced
X-064 | fixed | angzarr-project 4b2b3d8 | fact sequences 0-based; external_id on PageHeader
X-191 | fixed | angzarr-project b09b115 | client tier generic vocabulary; CommandHandlerClient naming; test-backend README
X-099 | fixed (spec) | angzarr-project 81d068a | rejection envelope documented; key = (target domain, FQ type)
X-126 | fixed (proto) | angzarr-project 81d068a | ComponentOptions.output_domains (tag 6)
X-192 | fixed | angzarr-project 81d068a | GetDescriptorRequest removed, sererr comment; -15 already fixed in b993c65
X-096 | fixed (spec) | angzarr-project 8ddc88c | temporal snapshot rule = core's; temporal_query.feature
X-022 | fixed (spec) | angzarr-project 86e9ad5 | canonical "angzarr", "" alias; main-timeline alias scenarios
X-063 | fixed (spec) | angzarr-project 492da1d | 2PC/sync modes/cascade error/DLQ/retention features
X-092 | fixed (spec) | angzarr-project 492da1d | retention rules specified; core must stop overwriting retention + prune DEFAULT
X-042 | fixed (spec half) | angzarr-project fcf3b8c | coordinator-contract README; client-rust sims still to delete
X-061 | fixed (proto half) | angzarr-project a73b739 | dlq_admin.proto moved; core build.rs must switch path
X-153 | wontfix (here) | - | example go_package handled by blackjack example
X-039 | fixed | angzarr-cli 9bf016b | unnamed stubs -> <Anchor>Impl in all 6 langs; ANZ012 for generated-type/proto-type collisions
X-040 | fixed | angzarr-cli 0a4a04e | scaffold requires out_dir=<out:>, stubs resolved there; refuses without it
X-105 | already-fixed | angzarr-cli 22deed6 (rem/l07-config) | explicit --config failure exits 1; comments cleaned
X-119 | fixed | angzarr-cli e582519 | l01 union in all 6 emitters + input_domain included in the filter
X-122 | fixed | angzarr-cli c673847 | l02 cross-category + projector Finish + per-language casing (snake/lowerFirst)
X-123 | fixed | angzarr-cli 007ded3, 8fbfe5a | full-registry rebuild of options.proto; ANZ009 on unresolvable option bytes
X-124 | fixed | angzarr-cli a1ea10c | ANZ013 on split runs + strategy: all documented/hinted
X-152 | fixed | angzarr-cli 66153ba | README + developer guide rewritten
X-175 | fixed (cli part) | angzarr-cli 88fc8aa | version stamped (l11); no fixed /tmp files; router/examples parts not in scope
X-186 | fixed | angzarr-cli 47c0158, d0498bd, 18d335c, 0a4a04e | F9-F15, F18 (comments, ANZ001, helpers, cppQuote, include, snakeToPascal)
