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
X-038 | fixed | angzarr-router deb6d3f, 359ed97, 7921e82, 42a46a3, b3c6896, ffc65c5, 6b00290, 315e3ab | ROUTER-01: per-component host state in all 6 bindings + identity PM routing; co-resident PM conformance
X-098 | fixed | angzarr-router deb6d3f, 359ed97 | ROUTER-02: PM rejections route by issuer/pm_domain, never fan out
X-024 | fixed (router part) | angzarr-router 9e74f78, c75075e | ROUTER-03: first escalation wins in compensator fan-out; client-rust part not in router
X-121 | fixed | angzarr-router d5a82ab, 6392b7b, ec84425 | ROUTER-04: idempotent close Java/TS; C# SafeHandle
X-120 | fixed | angzarr-router d793097, 6a9ef25, c3403f6, 237d4d5, f7816eb, 1bcba01 | ROUTER-05: ABI check in all bindings
X-130 | fixed | angzarr-router b4fd75f, 821efc3 | ROUTER-06: registry.rs in mutation set; 196/196 then 257 mutants, survivors killed
X-118 | fixed | angzarr-router c87f34c, 850cdae | ROUTER-07: docs true to code; client-rust planned on Rust-native API
X-166 | fixed | angzarr-router 40cbca2 | ROUTER-13
X-176 | fixed | angzarr-router e1ee937 | ROUTER-15
X-184 | fixed | angzarr-router 8ee881e, 7261418, a2efee3, f4e6ced, 2801c76, 54e0fe7, 6c92268 | ROUTER-08/09/10/11/12/14; register out-Status not added (ABI change)
X-185 | fixed | angzarr-router ac5b41b, b2bb90e, 50dbd93 | ROUTER-19
X-187 | fixed (router part) | angzarr-router 00c53f9, 6772e9c, a8011ef, 0aec12c, 0c9265a, 2225a87, e00ea1c, faa8b99 | ROUTER-16
X-198 | fixed | angzarr-router c87f34c, 764e23a | ROUTER-17
X-175 | blocked (router part) | - | ROUTER-18: no published toolchain tag set runs the recipes (go/java :latest lack angzarr CLI/JDK25, cpp lacks Catch2, only rust:v0.5.1-97 has cargo-mutants); needs angzarr-project image republish
