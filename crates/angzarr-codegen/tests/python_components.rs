//! The Python scaffold of every component kind (decision 14.50): a
//! projector over several source domains with an async logic, a policy with
//! its one refusal callback, a process manager with a fact, typed refusals
//! (several to a command, each under the status its message declares, the
//! domain's rejection codes as a Reason enum) and each emits cardinality,
//! the component's name from the proto (`component_name`, no default). Each generated file sits behind
//! angzarr-client-python's public API, so a hand-written one replaces it:
//! the generated code imports nothing else of the client.

mod common;

use angzarr_codegen::{
    CodegenError, Existing, MarkupError, Owner, ProtoInput, PythonApp, ScaffoldFile, complete,
    refusal_key, scaffold_python,
};
use common::*;

use test_dir::TestDir;

const LEDGER: &str = "io/angzarr/examples/v1/ledger.proto";
const POLICIES: &str = "io/angzarr/examples/v1/policies.proto";
const BUY_IN: &str = "io/angzarr/examples/v1/buy_in.proto";
const TABLE: &str = "io/angzarr/examples/v1/table.proto";
const TILL: &[&str] = &["pyshop/v1/till.proto"];
const KINDS: &str = "kinds/v1/kinds.proto";
const BARE: &[&str] = &["bare/v1/bare.proto"];
const STATUSES: &[&str] = &["statuses/v1/statuses.proto"];

fn app(domain: &str) -> PythonApp {
    PythonApp {
        domain: domain.to_string(),
        protos: ProtoInput::Directory("proto".to_string()),
        rules_version: "0.1.0".to_string(),
    }
}

fn blackjack(file: &str, domain: &str) -> Vec<ScaffoldFile> {
    scaffold_python(&descriptors(BLACKJACK), &[file], &app(domain))
        .unwrap_or_else(|e| panic!("{file} as {domain}: {e:?}"))
}

/// The source of the file of `files` the scaffold writes whose path ends
/// with `suffix`.
fn source<'a>(files: &'a [ScaffoldFile], suffix: &str) -> &'a str {
    &files
        .iter()
        .filter(|f| matches!(f.owner, Owner::User | Owner::Generated))
        .find(|f| f.path.ends_with(suffix))
        .unwrap_or_else(|| {
            panic!(
                "no {suffix}: {:?}",
                files.iter().map(|f| &f.path).collect::<Vec<_>>()
            )
        })
        .source
}

/// `text` with every run of whitespace one space and black's line
/// splitting undone (`(\n    a,\n)` as `(a)`), so a pin holds whatever
/// the width puts on a line.
fn flat(text: &str) -> String {
    let spaced = text.split_whitespace().collect::<Vec<_>>().join(" ");
    spaced
        .replace("( ", "(")
        .replace(" )", ")")
        .replace(",)", ")")
        .replace("{ ", "{")
        .replace(" }", "}")
        .replace(",}", "}")
}

fn assert_has(text: &str, expected: &[&str]) {
    let flattened = flat(text);
    for e in expected {
        assert!(flattened.contains(&flat(e)), "{e}:\n{text}");
    }
}

// ------------------------------------------------------------ projector

/// The wallet ledger: a projector of the wallet's events, named by its
/// `component_name` (decision 14.96: a spec projector has one source).
fn ledger() -> Vec<ScaffoldFile> {
    blackjack(LEDGER, "projector-wallet-ledger")
}

/// The kinds' ledger view: one projector of two source domains, its sources
/// derived from the owners of the events it names.
fn view() -> Vec<ScaffoldFile> {
    scaffold_python(
        &descriptors(&[KINDS]),
        &[KINDS],
        &app("projector-ledger-view"),
    )
    .unwrap()
}

#[test]
fn test_a_projector_is_scaffolded_under_its_component_name() {
    let files = ledger();
    let package = "src/angzarr_projector_wallet_ledger";
    for (path, owner) in [
        (format!("{package}/_gen/logic.py"), Owner::Generated),
        (format!("{package}/logic_impl.py"), Owner::User),
        (format!("{package}/_gen/domain.py"), Owner::Generated),
        (format!("{package}/_gen/main.py"), Owner::Generated),
        (format!("{package}/__main__.py"), Owner::User),
        (
            format!("{package}/_gen/io/angzarr/examples/v1/wallet_ledger_projection_angzarr.py"),
            Owner::Generated,
        ),
    ] {
        let found = files.iter().find(|f| f.path == path);
        assert_eq!(found.map(|f| f.owner), Some(owner), "{path}");
    }
}

#[test]
fn test_a_projectors_logic_projects_each_event_of_every_source_asynchronously() {
    let files = view();
    let logic = source(&files, "/logic.py");
    assert!(logic.contains("    PageContext,\n"), "{logic}");
    assert!(!logic.contains("CommandContext"), "{logic}");
    assert_has(
        logic,
        &[
            "class LedgerViewLogic(ABC):",
            "async def project_posted(self, projection: LedgerView, \
             event: Posted, ctx: PageContext) -> LedgerView: ...",
            "async def project_audited(self, projection: LedgerView, \
             event: Audited, ctx: PageContext) -> LedgerView: ...",
        ],
    );
    let implementation = source(&files, "/logic_impl.py");
    assert_has(
        implementation,
        &[
            "class LedgerViewLogicImpl(LedgerViewLogic):",
            "async def project_posted(self, projection: LedgerView, \
             event: Posted, ctx: PageContext) -> LedgerView: \
             raise NotImplementedError # TODO: project Posted",
        ],
    );
}

#[test]
fn test_a_projectors_dispatch_folds_both_sources_through_its_coroutines() {
    let files = view();
    let dispatch = source(&files, "ledger_view_angzarr.py");
    assert_has(
        dispatch,
        &[
            "def new_ledger_view_dispatch(logic: LedgerViewLogic) -> _az.ProjectorDispatch:",
            "dispatch = _az.ProjectorDispatch(\"projector-ledger-view\", _kinds.LedgerView)",
            "dispatch.for_domains(\"ledger\", \"audit\")",
            "projected = _run(logic.project_posted(_to_ledger_view(projection), event, ctx))",
            "projection.CopyFrom(_from_ledger_view(projected))",
            "dispatch.on_event(\"kinds.v1.Posted\", _project_posted)",
            "dispatch.on_event(\"kinds.v1.Audited\", _project_audited)",
            "return asyncio.run_coroutine_threadsafe(coroutine, _LOOP).result()",
        ],
    );
    let main = source(&files, "/main.py");
    assert_has(
        main,
        &["host.add_projector(new_ledger_view_dispatch(ledger_view_logic), DESCRIPTOR)"],
    );
}

/// A map field is a dict of the plain type, converted entry by entry.
#[test]
fn test_a_map_field_is_a_dict_of_its_plain_type() {
    let files = ledger();
    let domain = source(&files, "/domain.py");
    assert_has(
        domain,
        &[
            "from dataclasses import dataclass, field",
            "wallets: dict[str, WalletLedgerRow] = field(default_factory=dict)",
            "@dataclass(frozen=True) class WalletLedgerRow:",
            "from angzarr_client.router import PageContext as PageContext",
        ],
    );
    assert!(!domain.contains("CommandContext"), "{domain}");
    // A map's entry is no plain type of its own.
    assert!(!domain.contains("Entry"), "{domain}");
    let dispatch = source(&files, "wallet_ledger_projection_angzarr.py");
    assert_has(
        dispatch,
        &[
            "wallets={k: _to_wallet_ledger_row(v) for k, v in pb.wallets.items()},",
            "wallets={k: _from_wallet_ledger_row(v) for k, v in value.wallets.items()},",
        ],
    );
}

/// A command declaring no emits returns raw payloads of any of its
/// domain's events (unset is MANY): a list of any of them, typed, as Rust's
/// `Vec<LedgerStateEvent>`, not nothing.
#[test]
fn test_a_command_with_no_emits_returns_a_list_of_any_of_its_domains_events() {
    let files = scaffold_python(
        &descriptors(&["kinds/v1/kinds.proto"]),
        &["kinds/v1/kinds.proto"],
        &app("ledger"),
    )
    .unwrap();
    assert_has(
        source(&files, "/logic.py"),
        &[
            "def raw(self, cmd: Raw, state: LedgerState, ctx: CommandContext) \
           -> list[Posted | Noted | Cleared | Closed]: ...",
        ],
    );
}

#[test]
fn test_a_command_with_no_emits_of_a_domain_with_no_events_returns_none() {
    let files = scaffold_python(&descriptors(BARE), BARE, &app("bare")).unwrap();
    assert_has(
        source(&files, "/logic.py"),
        &[
            "def touch(self, cmd: Touch, state: BareState, ctx: CommandContext) -> None: ...",
            "def tap(self, cmd: Tap, state: BareState, ctx: CommandContext) -> None | TapRefused: ...",
        ],
    );
}

// --------------------------------------------------------------- policy

/// table -> player: each TurnStarted becomes the player's commands, each
/// its target root.
fn table_player() -> Vec<ScaffoldFile> {
    blackjack(POLICIES, "policy-table-player")
}

#[test]
fn test_a_policys_logic_reacts_to_each_event_with_its_targets_commands() {
    let files = table_player();
    let logic = source(&files, "/logic.py");
    assert_has(
        logic,
        &[
            "class TablePlayerPolicyLogic(ABC):",
            "def turn_started(self, event: TurnStarted, source: bytes, ctx: PageContext) \
             -> list[tuple[bytes, DecideMove | RecordMoveRefused]]: ...",
        ],
    );
    for import in [
        "    DecideMove,\n",
        "    PageContext,\n",
        "    TurnStarted,\n",
    ] {
        assert!(logic.contains(import), "{import}:\n{logic}");
    }
    assert!(!logic.contains("TablePlayerPolicy,"), "{logic}");
    // The player's DecideMove cannot be refused: the policy needs no path
    // for a refusal (C-0861), so no callback and no CommandContext.
    assert!(!logic.contains("refused("), "{logic}");
    assert!(!logic.contains("CommandContext"), "{logic}");
    let domain = source(&files, "/domain.py");
    assert_has(
        domain,
        &[
            "@dataclass(frozen=True) class DecideMove:",
            "from angzarr_client.router import PageContext as PageContext",
        ],
    );
    assert!(!domain.contains("CommandContext"), "{domain}");
}

/// A type its events and its target's commands both reach (`Card`, its
/// `Suit`) is declared, and converted, once: the commands are read from
/// the same protos as the events.
#[test]
fn test_a_policys_shared_types_are_declared_and_converted_once() {
    let files = table_player();
    let domain = source(&files, "/domain.py");
    for class in ["class Card:", "class Suit(enum.IntEnum):"] {
        assert_eq!(domain.matches(class).count(), 1, "{class}:\n{domain}");
    }
    let dispatch = source(&files, "table_player_policy_angzarr.py");
    for converter in ["def _to_card(pb)", "def _from_card(value"] {
        assert_eq!(
            dispatch.matches(converter).count(),
            1,
            "{converter}:\n{dispatch}"
        );
    }
}

#[test]
fn test_a_policys_dispatch_addresses_each_output_to_its_target() {
    let files = table_player();
    let dispatch = source(&files, "table_player_policy_angzarr.py");
    // One parameter too long for a line: black's split, a trailing comma.
    assert!(
        dispatch.contains(
            "def new_table_player_policy_dispatch(\n    logic: TablePlayerPolicyLogic,\n) -> _az.PolicyDispatch:\n"
        ),
        "{dispatch}"
    );
    assert_has(
        dispatch,
        &[
            "def new_table_player_policy_dispatch(logic: TablePlayerPolicyLogic) -> _az.PolicyDispatch:",
            "dispatch = _az.PolicyDispatch(\"policy-table-player\", \"table\", \"player\")",
            "book = _t.CommandBook(cover=_t.Cover(domain=\"player\"))",
            "book.cover.root.value = root",
            "return _outputs(logic.turn_started(event, source, ctx))",
            // DecideMove is a command of the player's (decision 14.69).
            "_COMMANDS = {_d.DecideMove: _from_decide_move,}",
            "_SENT_FACTS = {_d.RecordMoveRefused: _from_record_move_refused,}",
            "book.page.command.payload.CopyFrom(_az.pack(to_wire(output)))",
            "dispatch.on_event(\"io.angzarr.examples.v1.TurnStarted\", _turn_started)",
        ],
    );
    // Nothing it sends can be refused: no refusal path in the dispatch.
    assert!(!dispatch.contains("callback's arguments"), "{dispatch}");
    for gone in ["on_refused", "_typed_refusal", "_fact(", "_FACTS = "] {
        assert!(
            !dispatch.replace("_SENT_FACTS = ", "").contains(gone),
            "{gone}:\n{dispatch}"
        );
    }
    let main = source(&files, "/main.py");
    assert_has(
        main,
        &[
            "host.add_policy(new_table_player_policy_dispatch(table_player_policy_logic), DESCRIPTOR)",
        ],
    );
}

// ------------------------------------------------------ process manager

fn buy_in() -> Vec<ScaffoldFile> {
    blackjack(BUY_IN, "buy-in")
}

/// The process manager is a domain: decisions and folds as an
/// aggregate's, its fact recorded as received and flagged by what it
/// emits, served as the client's `ProcessManagerDispatch`.
#[test]
fn test_a_process_manager_is_scaffolded_with_its_fact() {
    let files = buy_in();
    let logic = source(&files, "/logic.py");
    assert_has(
        logic,
        &[
            "class BuyInLogic(ABC):",
            // A fact the table's policy sends (ruling 9, decision 14.69).
            "def start_buy_in(self, fact: StartBuyIn, state: BuyInState, ctx: CommandContext) \
             -> list[BuyInStarted]: ...",
            "def seat_confirmation_refused(self, fact: SeatConfirmationRefused, state: BuyInState, \
             ctx: CommandContext) -> list[BuyInSeatLost | HoldReleaseDue | SeatReleaseDue]: ...",
        ],
    );
    let dispatch = source(&files, "buy_in_state_angzarr.py");
    assert_has(
        dispatch,
        &[
            "dispatch = _az.ProcessManagerDispatch(\"pmg-buy-in\", \"buy-in\", rebuilder)",
            "return _az.FactRecord(wire, tuple(_wire(flags)))",
            "dispatch.on_fact(\"io.angzarr.examples.v1.SeatConfirmationRefused\", _on_seat_confirmation_refused)",
        ],
    );
    let main = source(&files, "/main.py");
    assert_has(
        main,
        &["host.add_process_manager(new_buy_in_state_dispatch(buy_in_logic), DESCRIPTOR)"],
    );
    let domain = source(&files, "/domain.py");
    assert_has(domain, &["class BuyInStatePhase(enum.IntEnum):"]);
    // Its starting types are in its descriptor, beside its domain.
    let descriptor = source(&files, "/_descriptor.py");
    let hex: String = descriptor
        .lines()
        .filter_map(|l| l.strip_prefix("        \"")?.strip_suffix('"'))
        .collect();
    assert!(
        hex.contains(&hex_of("io.angzarr.examples.v1.StartBuyIn")),
        "{hex}"
    );
}

// ------------------------------------------------ refusals, lists

fn till() -> Vec<ScaffoldFile> {
    scaffold_python(&descriptors(TILL), TILL, &app("till")).unwrap()
}

/// A decision returns a list of its events (decision 14.75; C-0797) or one
/// of its typed refusals, several to a command.
#[test]
fn test_a_decision_returns_a_list_or_one_of_its_refusals() {
    let files = till();
    let logic = source(&files, "/logic.py");
    assert_has(
        logic,
        &[
            "def charge(self, cmd: Charge, state: TillState, ctx: CommandContext) \
             -> list[Charged] | TillRefused | CardDeclined: ...",
            "def refund(self, cmd: Refund, state: TillState, ctx: CommandContext) \
             -> list[Refunded] | TillRefused: ...",
            "def settle(self, cmd: Settle, state: TillState, ctx: CommandContext) \
             -> list[Charged | Refunded]: ...",
            "def open(self, cmd: Open, state: TillState, ctx: CommandContext) \
             -> list[Charged | Refunded]: ...",
            "def close(self, cmd: Close, state: TillState, ctx: CommandContext) \
             -> list[Charged | Refunded] | Unbalanced: ...",
            "def cash_counted(self, fact: CashCounted, state: TillState, ctx: CommandContext) \
             -> list[Refunded]: ...",
        ],
    );
}

/// The dispatch answers a returned refusal as the client's `Refused`, the
/// refusal on the wire, under its code's status (decision 14.52).
#[test]
fn test_the_dispatch_refuses_with_a_returned_typed_refusal() {
    let files = till();
    let dispatch = source(&files, "till_state_angzarr.py");
    assert_has(
        dispatch,
        &[
            "_REFUSALS = {_d.TillRefused: _from_till_refused, _d.CardDeclined: _from_card_declined, _d.Unbalanced: _from_unbalanced,}",
            "to_wire = _REFUSALS.get(type(result))",
            "raise _az.Refused(to_wire(result), result.status())",
            "return _decided(logic.charge(cmd, _to_till_state(state), cctx))",
            // A command refusing nothing answers its events as before.
            "return _book(logic.settle(cmd, _to_till_state(state), cctx))",
            // Each event is the page's `event` arm.
            "book.pages.add().event.CopyFrom(_az.pack(wire))",
        ],
    );
    assert!(
        !dispatch.contains(".payload.CopyFrom(_az.pack(wire))"),
        "{dispatch}"
    );
    // The client's Refused takes the status: nothing sets it afterwards.
    assert!(!dispatch.contains(".grpc"), "{dispatch}");
}

/// A refusal's codes are its Reason enum, each value by its name without
/// the enum's prefix (the ErrorInfo.reason on the wire); its diagnostic
/// key is the refusal's full name and its reason, its args its set fields,
/// and its status is its message's `(refusal_status)`, whatever its reason:
/// the till's refusal is OUT_OF_RANGE (11) for every code, the card's
/// FAILED_PRECONDITION (9).
#[test]
fn test_a_refusals_reason_enum_is_its_rejection_code() {
    let files = till();
    let domain = source(&files, "/domain.py");
    assert_has(
        domain,
        &[
            "class TillRefusedReason(enum.IntEnum): UNSPECIFIED = 0 TILL_CLOSED = 1 OVER_LIMIT = 2",
            "reason: TillRefusedReason = TillRefusedReason.UNSPECIFIED",
            "limit: int | None = None",
            "def key(self) -> str:",
            "return {TillRefusedReason.TILL_CLOSED: \"pyshop-v1-tillrefused-till-closed\", \
             TillRefusedReason.OVER_LIMIT: \"pyshop-v1-tillrefused-over-limit\",}\
             .get(self.reason, \"pyshop-v1-tillrefused\")",
            "def args(self) -> dict[str, object]:",
            "def status(self) -> int:",
        ],
    );
    let out = run_domain(
        domain,
        "r = TillRefused(reason=TillRefusedReason.OVER_LIMIT, limit=5)\n\
         assert r.status() == 11, r.status()\n\
         assert TillRefused(reason=TillRefusedReason.TILL_CLOSED).status() == 11\n\
         assert TillRefused().status() == 11\n\
         assert CardDeclined().status() == 9\n\
         assert r.key() == 'pyshop-v1-tillrefused-over-limit', r.key()\n\
         assert r.args() == {'reason': 'OVER_LIMIT', 'limit': 5}, r.args()\n\
         assert TillRefused(reason=TillRefusedReason.TILL_CLOSED).args() == {'reason': 'TILL_CLOSED'}\n\
         d = CardDeclined(issuer='visa')\n\
         assert d.key() == 'pyshop-v1-carddeclined', d.key()\n\
         assert d.args() == {'issuer': 'visa'}, d.args()\n\
         assert CardDeclined().args() == {}\n\
         u = Unbalanced(reason='short')\n\
         assert u.key() == 'pyshop-v1-unbalanced', u.key()\n\
         assert u.args() == {'reason': 'short'}, u.args()\n\
         print('ok')\n",
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout), "ok\n");
}

/// A typed refusal's key is the same string in Python as in Rust
/// (`angzarr_codegen::refusal_key`, what the Rust domain layer's
/// `Diagnostic::code` is): a nested `Reason` held as `reason` keys by its
/// wire name, and a `reason` of any other enum (not nested, of another
/// name) keys as the refusal alone.
#[test]
fn test_a_refusals_key_is_the_same_in_python_as_in_rust() {
    const KINDS: &str = "kinds/v1/kinds.proto";
    let files = scaffold_python(&descriptors(&[KINDS]), &[KINDS], &app("ledger")).unwrap();
    let domain = source(&files, "/domain.py");
    let cases = [
        (
            "PostRefused(reason=PostRefusedReason.CLOSED)",
            refusal_key("kinds.v1.PostRefused", Some("CLOSED")),
        ),
        (
            "PostRefused(reason=PostRefusedReason.OVER_LIMIT)",
            refusal_key("kinds.v1.PostRefused", Some("OVER_LIMIT")),
        ),
        (
            "Unreasoned(reason=Tone.LOUD)",
            refusal_key("kinds.v1.Unreasoned", None),
        ),
        (
            "Mislabeled(reason=MislabeledCause.LATE)",
            refusal_key("kinds.v1.Mislabeled", None),
        ),
        (
            "Elsewhere(why=ElsewhereReason.AWAY)",
            refusal_key("kinds.v1.Elsewhere", None),
        ),
    ];
    let script: String = cases
        .iter()
        .map(|(value, key)| format!("assert {value}.key() == {key:?}, {value}.key()\n"))
        .chain(["print('ok')\n".to_string()])
        .collect();
    let out = run_domain(domain, &script);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout), "ok\n");
    let rust = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/golden/kinds.domain.rs"
    ))
    .unwrap();
    for (_, key) in &cases {
        assert!(rust.contains(&format!("{key:?}")), "{key} in Rust");
    }
}

#[test]
fn test_the_spec_table_is_scaffolded() {
    let files = blackjack(TABLE, "table");
    let logic = source(&files, "/logic.py");
    assert_has(
        logic,
        &[
            "def place_bet(self, cmd: PlaceBet, state: TableState, ctx: CommandContext) \
             -> list[BetPlaced] | TableNotFound | NotSeated | RoundInProgress | AlreadyBet | InsufficientStack | BetOutOfRange | BetNotEven: ...",
            "def confirm_seat(self, cmd: ConfirmSeat, state: TableState, ctx: CommandContext) \
             -> list[PlayerSeated] | SeatNotConfirmable: ...",
            // A fact: the buy-in's compensation (ruling 9, decision 14.69).
            "def release_seat(self, fact: ReleaseSeat, state: TableState, ctx: CommandContext) \
             -> list[SeatReleased]: ...",
            "def hit(self, cmd: Hit, state: TableState, ctx: CommandContext) \
             -> list[CardDealt | TurnStarted | DealerPlayed | RoundSettled] | TableNotFound | NoRoundInProgress | NotYourTurn: ...",
        ],
    );
    let dispatch = source(&files, "table_state_angzarr.py");
    assert_has(
        dispatch,
        &["_az.AggregateDispatch(\"table\", \"table\", rebuilder)"],
    );
}

// --------------------------------------------------------------- names

#[test]
fn test_each_component_takes_its_name_from_the_proto() {
    for (files, dispatch, name) in [
        (
            ledger(),
            "wallet_ledger_projection_angzarr.py",
            "projector-wallet-ledger",
        ),
        (view(), "ledger_view_angzarr.py", "projector-ledger-view"),
        (
            table_player(),
            "table_player_policy_angzarr.py",
            "policy-table-player",
        ),
        (buy_in(), "buy_in_state_angzarr.py", "pmg-buy-in"),
        (till(), "till_state_angzarr.py", "till"),
    ] {
        let source_ = source(&files, dispatch);
        assert!(
            flat(source_).contains(&format!("Dispatch(\"{name}\"")),
            "{name}:\n{source_}"
        );
        let descriptor = source(&files, "/_descriptor.py");
        let hex: String = descriptor
            .lines()
            .filter_map(|l| l.strip_prefix("        \"")?.strip_suffix('"'))
            .collect();
        let named = format!("0a{:02x}{}", name.len(), hex_of(name));
        assert!(hex.starts_with(&named), "{name}: {hex}");
    }
}

fn hex_of(text: &str) -> String {
    text.bytes().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn test_a_component_declaring_no_name_is_refused_with_no_default() {
    const FILE: &str = "bad/v1/component_name_empty.proto";
    assert_eq!(
        scaffold_python(&descriptors(&[FILE]), &[FILE], &app("things")),
        Err(CodegenError::Markup(MarkupError::ComponentName {
            component: "bad.v1.ThingState".to_string(),
            name: String::new(),
        }))
    );
}

/// A projector's sources are its events' domains, in order, and its name its
/// `component_name`.
#[test]
fn test_a_projectors_sources_are_its_events_domains() {
    const FILE: &str = "pyshop/v1/views.proto";
    let files = scaffold_python(
        &descriptors(&[FILE]),
        &[FILE],
        &app("projector-http-server-view"),
    )
    .unwrap_or_else(|e| panic!("{e:?}"));
    let dispatch = source(&files, "_angzarr.py");
    assert!(
        flat(dispatch).contains("_az.ProjectorDispatch(\"projector-http-server-view\""),
        "{dispatch}"
    );
    assert!(
        flat(dispatch).contains("dispatch.for_domains(\"pages\")"),
        "{dispatch}"
    );
}

// ------------------------------------------------- hand-replaceability

/// What the generated code may use of angzarr-client-python: its public
/// API only, so a hand-written file in its place is written against the
/// same. Each import of `angzarr_client`, and each attribute of the
/// router (`_az.`), is one of these.
const PUBLIC_IMPORTS: &[&str] = &[
    "from angzarr_client import Compensation as Compensation",
    "from angzarr_client import ComponentHost, configure_logging",
    "from angzarr_client.child import run_child",
    "from angzarr_client.proto.io.angzarr.v1 import types_pb2 as _t",
    "import angzarr_client.router as _az",
    "from angzarr_client.router import Bug as Bug",
    "from angzarr_client.router import CommandContext as CommandContext",
    "from angzarr_client.router import PageContext as PageContext",
];
/// `angzarr_client.router.__all__` at the pinned client.
const ROUTER_ALL: &[&str] = &[
    "WILDCARD_DOMAIN",
    "AggregateDispatch",
    "Bug",
    "CodedError",
    "CommandContext",
    "Conflict",
    "FactRecord",
    "GrpcCode",
    "Immediate",
    "PageContext",
    "PolicyDispatch",
    "ProcessManagerDispatch",
    "ProjectorDispatch",
    "Rebuilder",
    "Refused",
    "Rejection",
    "RetryableBug",
    "Router",
    "UnclassifiedError",
    "any_decode_error",
    "current_cover",
    "current_page",
    "pack",
];

#[test]
fn test_the_generated_code_uses_only_the_clients_public_api() {
    for files in [
        ledger(),
        view(),
        table_player(),
        buy_in(),
        till(),
        buyin_python("policy-buyin-wallet"),
    ] {
        for f in files.iter().filter(|f| f.path.ends_with(".py")) {
            for line in f.source.lines() {
                let import = line.starts_with("from angzarr_client")
                    || line.starts_with("import angzarr_client");
                assert!(
                    !import || PUBLIC_IMPORTS.contains(&line),
                    "{}: {line}",
                    f.path
                );
            }
            for (at, _) in f.source.match_indices("_az.") {
                let name: String = f.source[at + 4..]
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                assert!(
                    ROUTER_ALL.contains(&name.as_str()),
                    "{}: _az.{name}",
                    f.path
                );
            }
        }
    }
}

/// The user's files of every kind name nothing of the harness.
#[test]
fn test_the_users_files_of_every_kind_name_nothing_of_the_harness() {
    for files in [
        ledger(),
        view(),
        table_player(),
        buy_in(),
        till(),
        buyin_python("policy-buyin-wallet"),
    ] {
        for f in files
            .iter()
            .filter(|f| f.owner == Owner::User && f.path.ends_with(".py"))
        {
            for harness in ["angzarr_client", "ComponentHost", "_az", "_pb2", "protobuf"] {
                assert!(!f.source.contains(harness), "{} names {harness}", f.path);
            }
        }
    }
}

/// Runs `script` with python3 beside `domain` (a generated `domain.py`),
/// an `angzarr_client.router` stub standing in for the client.
fn run_domain(domain: &str, script: &str) -> std::process::Output {
    let dir = TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-python-components"));
    let client = dir.join("angzarr_client");
    std::fs::create_dir_all(&client).unwrap();
    std::fs::write(client.join("__init__.py"), "").unwrap();
    std::fs::write(
        client.join("router.py"),
        "class Bug(Exception): ...\nclass CommandContext: ...\nclass PageContext: ...\n",
    )
    .unwrap();
    std::fs::write(dir.join("domain.py"), domain).unwrap();
    let main = dir.join("check.py");
    std::fs::write(
        &main,
        format!(
            "import sys\nsys.path.insert(0, {:?})\nfrom domain import *\n{script}",
            dir.display().to_string()
        ),
    )
    .unwrap();
    std::process::Command::new("python3")
        .arg("-I")
        .arg(&main)
        .output()
        .expect("python3 on PATH")
}

/// buf generates every proto the reached ones import, used or not: a
/// pb2 module imports each of its file's imports (buy_in.proto imports
/// wallet.proto, none of whose messages the buy-in reaches).
#[test]
fn test_buf_generates_every_import_of_a_reached_proto() {
    let files = buy_in();
    let buf = source(&files, "buf.gen.yaml");
    for proto in [BUY_IN, "io/angzarr/examples/v1/wallet.proto"] {
        assert!(
            buf.contains(&format!("      - proto/{proto}\n")),
            "{proto}:\n{buf}"
        );
    }
    assert!(!buf.contains("io/angzarr/v1/"), "{buf}");
}

/// A projector naming its one source as `input_domain` reads that domain.
#[test]
fn test_a_projectors_input_domain_is_its_one_source() {
    const FILE: &str = "pyshop/v1/views.proto";
    let files = scaffold_python(
        &descriptors(&[FILE]),
        &[FILE],
        &app("projector-page-counter"),
    )
    .unwrap_or_else(|e| panic!("{e:?}"));
    let dispatch = source(&files, "_angzarr.py");
    assert!(
        flat(dispatch).contains("dispatch.for_domains(\"pages\")"),
        "{dispatch}"
    );
}

/// A map of an enum converts each value through the enum, a map of a
/// scalar as it is; a well-known type is never a plain type of the domain.
#[test]
fn test_a_map_converts_each_value_by_its_kind() {
    let files = till();
    let dispatch = source(&files, "till_state_angzarr.py");
    assert_has(
        dispatch,
        &[
            "drawers={k: _d.Drawer(v) for k, v in pb.drawers.items()},",
            "drawers={k: int(v) for k, v in value.drawers.items()},",
            "floats=dict(pb.floats)",
            "floats=dict(value.floats)",
        ],
    );
    let domain = source(&files, "/domain.py");
    assert!(
        domain.contains("    drawers: dict[str, Drawer] = field(default_factory=dict)\n"),
        "{domain}"
    );
    assert!(!domain.contains("class Timestamp"), "{domain}");
}

/// A policy whose target takes facts only sends those facts, each as a
/// Fact (decision 14.69): its reactions return them, and its dispatch
/// sends none as a command.
#[test]
fn test_a_policy_whose_target_takes_facts_only_sends_its_facts() {
    const FILE: &str = "pyshop/v1/views.proto";
    let files = scaffold_python(&descriptors(&[FILE]), &[FILE], &app("policy-pages-tally"))
        .unwrap_or_else(|e| panic!("{e:?}"));
    let logic = source(&files, "/logic.py");
    assert_has(logic, &["-> list[tuple[bytes, Tallied]]: ..."]);
    let dispatch = source(&files, "pages_tally_policy_angzarr.py");
    assert_has(
        dispatch,
        &[
            "_COMMANDS = {}",
            "_SENT_FACTS = {_d.Tallied: _from_tallied,}",
            "to_wire = _COMMANDS.get(type(output))",
            "wire = _SENT_FACTS[type(output)](output)",
            "book.page.fact.payload.CopyFrom(_az.pack(wire))",
        ],
    );
    assert_parses("pages_tally_policy_angzarr.py", dispatch);
}

/// A projector's descriptor names its sources in its dispatch's order
/// (`input_domains`'), so the client's `describe` of the dispatch is the
/// same descriptor.
#[test]
fn test_a_projectors_descriptor_lists_its_sources_in_order() {
    let files = view();
    let descriptor = source(&files, "/_descriptor.py");
    let hex: String = descriptor
        .lines()
        .filter_map(|l| l.strip_prefix("        \"")?.strip_suffix('"'))
        .collect();
    // The projector's arm of the descriptor: its input domains, each a
    // string, in the order of its dispatch's.
    let sources = |domains: &[&str]| {
        let inner: String = domains
            .iter()
            .map(|d| format!("0a{:02x}{}", d.len(), hex_of(d)))
            .collect();
        format!("32{:02x}{inner}", inner.len() / 2)
    };
    assert!(hex.contains(&sources(&["ledger", "audit"])), "{hex}");
}

// ------------------------------------------------------------- refusals

/// The toy buy-in (`tests/fixtures/buyin`): the buy-in process manager is
/// the source of the BuyInWallet policy, whose target, the wallet, takes
/// Deposit and Hold, Hold refusing with InsufficientFunds.
fn buyin_python(domain: &str) -> Vec<ScaffoldFile> {
    scaffold_python(&descriptors(BUYIN), BUYIN, &app(domain))
        .unwrap_or_else(|e| panic!("{domain}: {e:?}"))
}

/// `source` parses as Python (python3's `ast`).
fn assert_parses(path: &str, source: &str) {
    let out = std::process::Command::new("python3")
        .arg("-I")
        .arg("-c")
        .arg("import ast, sys; ast.parse(sys.stdin.read())")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            child.stdin.take().unwrap().write_all(source.as_bytes())?;
            child.wait_with_output()
        })
        .expect("python3 on PATH");
    assert!(
        out.status.success(),
        "{path}: {}\n{source}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn test_a_domain_is_handed_no_pass_through_refusal() {
    let files = buyin_python("buyin");
    let logic = source(&files, "/logic.py");
    // Retired (C-0613): the refusal of a command a policy sends goes to
    // that policy's one callback, never to the source domain: no
    // `on_refused_<command>`, no reference protocol, no optional method,
    // and none of the target's types in the source's plain types.
    for gone in ["on_refused", "Protocol", "BuyInRefusals", "refused("] {
        assert!(!logic.contains(gone), "{gone}:\n{logic}");
    }
    let implementation = implementation(&files);
    assert!(!implementation.source.contains("on_refused"));
    assert!(!implementation.source.contains("optional"));
    assert!(implementation.optional.is_empty());
    assert!(implementation.selected.is_empty());
    let domain = source(&files, "/domain.py");
    for foreign in ["class Hold:", "class Deposit:", "class InsufficientFunds:"] {
        assert!(!domain.contains(foreign), "{foreign}:\n{domain}");
    }
    let dispatch = source(&files, "buy_in_state_angzarr.py");
    for gone in ["on_refused", "_typed_refusal", "_TYPED_REFUSALS", "hasattr"] {
        assert!(!dispatch.contains(gone), "{gone}:\n{dispatch}");
    }
    assert_parses("buy_in_state_angzarr.py", dispatch);
    assert_parses("logic.py", logic);
}

#[test]
fn test_a_policy_must_translate_each_refused_command() {
    let files = buyin_python("policy-buyin-wallet");
    let logic = source(&files, "/logic.py");
    assert!(!logic.contains("(Protocol):"), "{logic}");
    // One callback, an abstract method: the command refused and the
    // refusal, each a choice (a union a type checker holds exhaustive) of
    // the target's commands and of their refusal messages, each once, and
    // one of the source's facts out. It cannot decline: no None.
    assert_has(
        logic,
        &[
            "@abstractmethod def refused(self, command: Deposit | Hold, refusal: DepositRefused | InsufficientFunds, ctx: CommandContext) -> HoldLost: ...",
        ],
    );
    assert_eq!(logic.matches("def refused(").count(), 1, "{logic}");
    assert!(!logic.contains("_refused("), "{logic}");
    assert!(!logic.contains("| None"), "{logic}");
    let implementation = source(&files, "/logic_impl.py");
    assert_has(
        implementation,
        &[
            "def refused(self, command: Deposit | Hold, refusal: DepositRefused | InsufficientFunds, ctx: CommandContext) -> HoldLost: \
             raise NotImplementedError # TODO: translate the refusal",
        ],
    );
    assert_eq!(implementation.matches("def refused(").count(), 1);
    let domain = source(&files, "/domain.py");
    assert!(
        domain.contains("from angzarr_client.router import CommandContext as CommandContext"),
        "{domain}"
    );
    let dispatch = source(&files, "buy_in_wallet_policy_angzarr.py");
    assert_has(
        dispatch,
        &[
            "_FACTS = {_d.HoldLost: _from_hold_lost,}",
            "_TARGET_COMMANDS = {\"buyin.v1.Deposit\": (_wallet.Deposit, _to_deposit), \"buyin.v1.Hold\": (_wallet.Hold, _to_hold),}",
            "def _refused(refusal, ctx):",
            "command = _target_command(refusal.command.page.command.payload)",
            "typed = _typed_refusal(refusal.response.refusal)",
            "return _fact(logic.refused(command, typed, ctx))",
            "dispatch.on_refused(\"buyin.v1.Deposit\", _refused)",
            "dispatch.on_refused(\"buyin.v1.Hold\", _refused)",
            // A refusal that is not a typed one of the target is a bug, never
            // a declined translation.
            "raise _az.Bug(\"refused-command-unknown\")",
            "raise _az.Bug(\"refusal-without-object\")",
            "raise _az.Bug(\"refusal-unknown\")",
        ],
    );
    assert!(!dispatch.contains("hasattr"), "{dispatch}");
    assert_has(
        dispatch,
        &["the refusal of a command it sent into its one callback's arguments"],
    );
    assert_parses("buy_in_wallet_policy_angzarr.py", dispatch);
    assert_parses("logic.py", logic);
}

#[test]
fn test_a_policy_translates_into_its_sources_fact() {
    let files = blackjack(POLICIES, "policy-player-table");
    let domain = source(&files, "/domain.py");
    assert!(domain.contains("CommandContext"), "{domain}");
    let logic = source(&files, "/logic.py");
    assert_eq!(logic.matches("def refused(").count(), 1, "{logic}");
    assert_has(logic, &["-> RecordMoveRefused: ..."]);
    let dispatch = source(&files, "player_table_policy_angzarr.py");
    assert_has(
        dispatch,
        &[
            "_FACTS = {_d.RecordMoveRefused: _from_record_move_refused,}",
            "dispatch.on_refused(\"io.angzarr.examples.v1.Hit\", _refused)",
            "dispatch.on_refused(\"io.angzarr.examples.v1.ConfirmSeat\", _refused)",
        ],
    );
    assert_eq!(dispatch.matches("def _refused(").count(), 1);
}

/// The implementation's file, by suffix.
fn implementation(files: &[ScaffoldFile]) -> &ScaffoldFile {
    files
        .iter()
        .find(|f| f.path.ends_with("/logic_impl.py"))
        .expect("logic_impl.py")
}

#[test]
fn test_a_callback_returns_a_choice_of_the_sources_facts() {
    let files = scaffold_python(
        &descriptors(&[KINDS]),
        &[KINDS],
        &app("policy-ledger-audit"),
    )
    .unwrap();
    // The ledger takes two facts, and the audit's one command refuses two
    // types: a choice each way.
    assert_has(
        source(&files, "/logic.py"),
        &[
            "def refused(self, command: Audit, refusal: PostRefused | Unreasoned, ctx: CommandContext) -> Arrived | Swept: ...",
        ],
    );
    let implementation = implementation(&files);
    assert_has(
        &implementation.source,
        &["ctx: CommandContext) -> Arrived | Swept:"],
    );
    assert!(implementation.selected.is_empty());
    assert!(implementation.optional.is_empty());
}

/// A refusal is refused under the status its message declares
/// (`(refusal_status)`, decision 14.95; there is no default), one number
/// each of the six business-fatal statuses and ABORTED (decision 14.111;
/// `google.rpc.Code`).
#[test]
fn test_a_refusal_is_refused_under_the_status_its_message_declares() {
    let files = scaffold_python(&descriptors(STATUSES), STATUSES, &app("gate")).unwrap();
    let domain = source(&files, "/domain.py");
    let out = run_domain(
        domain,
        "assert NotInRange().status() == 11\n\
         assert BadInput().status() == 3\n\
         assert Missing().status() == 5\n\
         assert Taken().status() == 6\n\
         assert Forbidden().status() == 7\n\
         assert NotReady().status() == 9\n\
         assert Stale().status() == 10\n\
         print('ok')\n",
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout), "ok\n");
    // Read from the option: no default number stands in the source.
    assert!(
        !domain.contains("FAILED_PRECONDITION (9) where unmarked"),
        "{domain}"
    );
}

/// `--missing` keys a policy's one refusal callback by its name, not by the
/// type of its input (a choice, which a user may alias): absent, it is
/// added at the class's end; present, however its input is annotated, it is
/// neither added again nor reported obsolete.
#[test]
fn test_complete_finds_a_policys_refusal_callback_by_its_name() {
    let fresh = implementation(&buyin_python("policy-buyin-wallet")).clone();
    let callback = "    def refused(\n        self,\n        command: Deposit | Hold,\n        refusal: DepositRefused | InsufficientFunds,\n        ctx: CommandContext,\n    ) -> HoldLost:\n        raise NotImplementedError  # TODO: translate the refusal\n";
    assert!(fresh.source.contains(callback), "{}", fresh.source);
    let without = fresh.source.replace(&format!("\n{callback}"), "");
    assert_ne!(without, fresh.source);
    let added = complete(&fresh, &without, Existing::Complete).unwrap();
    assert_eq!(
        added.completed,
        Some(format!("{without}\n{callback}")),
        "{without}"
    );
    let aliased = fresh.source.replace(
        "command: Deposit | Hold,\n        refusal: DepositRefused | InsufficientFunds,",
        "command: WalletCommand,\n        refusal: WalletRefusal,",
    );
    assert_ne!(aliased, fresh.source);
    let kept = complete(&fresh, &aliased, Existing::Complete).unwrap();
    assert_eq!(kept.completed, None);
    assert!(kept.notes.obsolete.is_empty(), "{:?}", kept.notes);
    assert!(kept.notes.lacks.is_empty(), "{:?}", kept.notes);
}

/// A policy whose target cannot refuse needs no path for a refusal
/// (C-0861): no callback in its interface, no registration in its dispatch.
#[test]
fn test_a_policy_whose_target_cannot_refuse_has_no_refusal_callback() {
    const UNREFUSED: &[&str] = &["unrefused/v1/unrefused.proto"];
    let files = scaffold_python(
        &descriptors(UNREFUSED),
        UNREFUSED,
        &app("policy-order-inventory"),
    )
    .unwrap();
    for (suffix, gone) in [
        ("/logic.py", "refused("),
        ("/logic_impl.py", "refused("),
        ("order_to_inventory_policy_angzarr.py", "on_refused"),
        ("order_to_inventory_policy_angzarr.py", "_typed_refusal"),
        ("/domain.py", "CommandContext"),
    ] {
        let found = source(&files, suffix);
        assert!(!found.contains(gone), "{suffix} has {gone}:\n{found}");
    }
}
