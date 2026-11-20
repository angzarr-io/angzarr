//! The scaffold, split so the user never edits a generated file and a run
//! never rewrites the user's: per component its interface (a trait over the
//! domain types) in the generated domain layer, and its implementation, the
//! user's, written once (`angzarr scaffold rust`) and never overwritten,
//! `--missing` adding the required methods it lacks; and the adapters, the
//! CLI's, regenerated every build, carrying angzarr-embedded's markup and
//! delegating to the interface. Pinned by golden files
//! (`tests/golden/rust/buyin`, `tests/golden/buyin.adapters.rs`); that the
//! adapters compile against angzarr-embedded is angzarr-build's
//! `tests/local.rs`.

mod common;

use angzarr_codegen::{
    CodegenError, Existing, Notes, Owner, ScaffoldFile, UseError, adapters, complete,
    markup_domain, scaffold,
};
use common::*;

const GOLDEN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden");

/// The buy-in project's scaffold.
fn buyin() -> Vec<ScaffoldFile> {
    scaffold(&descriptors(BUYIN), BUYIN).unwrap()
}

/// The domain layer of `files`' one package, its interfaces in it.
fn layer(files: &[&str], package: &str) -> String {
    markup_domain(&descriptors(files), files, &[(package, "crate::wire")])
        .unwrap()
        .into_iter()
        .find(|g| g.package == package)
        .map(|g| g.source)
        .unwrap_or_else(|| panic!("no layer of {package}"))
}

/// The buy-in's domain layer.
fn buyin_layer() -> String {
    layer(BUYIN, "buyin.v1")
}

fn file_at<'a>(files: &'a [ScaffoldFile], path: &str) -> &'a ScaffoldFile {
    files
        .iter()
        .find(|f| f.path == path)
        .unwrap_or_else(|| panic!("no {path}: {files:#?}"))
}

/// `actual`, against the golden file at `path` under `tests/golden`;
/// `ANGZARR_BLESS=1` rewrites the golden file instead.
fn golden(path: &str, actual: &str) {
    let path = format!("{GOLDEN}/{path}");
    if std::env::var_os("ANGZARR_BLESS").is_some() {
        std::fs::write(&path, actual).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|_| panic!("no {path}"));
    assert_eq!(actual, expected, "{path}");
}

#[test]
fn test_scaffold_writes_an_implementation_per_component_and_reports_the_old_interface() {
    let mut placed: Vec<(String, String, Owner)> = buyin()
        .into_iter()
        .map(|f| (f.component, f.path, f.owner))
        .collect();
    placed.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    let at = |c: &str, p: &str, o: Owner| (c.to_string(), p.to_string(), o);
    assert_eq!(
        placed,
        [
            at("buyin.v1.BuyInState", "buy_in_state.rs", Owner::Obsolete),
            at("buyin.v1.BuyInState", "buy_in_state_impl.rs", Owner::User),
            at(
                "buyin.v1.BuyInWalletPolicy",
                "buy_in_wallet_policy.rs",
                Owner::Obsolete
            ),
            at(
                "buyin.v1.BuyInWalletPolicy",
                "buy_in_wallet_policy_impl.rs",
                Owner::User
            ),
            at(
                "buyin.v1.WalletBuyInPolicy",
                "wallet_buy_in_policy.rs",
                Owner::Obsolete
            ),
            at(
                "buyin.v1.WalletBuyInPolicy",
                "wallet_buy_in_policy_impl.rs",
                Owner::User
            ),
            at("buyin.v1.WalletState", "wallet_state.rs", Owner::Obsolete),
            at("buyin.v1.WalletState", "wallet_state_impl.rs", Owner::User),
        ]
    );
}

#[test]
fn test_scaffold_matches_the_golden_files() {
    for f in buyin().iter().filter(|f| f.owner == Owner::User) {
        golden(&format!("rust/buyin/{}", f.path), &f.source);
    }
}

/// Each component's interface is the domain layer's: the trait the
/// adapter delegates to and the implementation implements, generated with
/// the protos.
#[test]
fn test_the_interfaces_are_the_domain_layers() {
    let layer = buyin_layer();
    for name in [
        "BuyInStateInterface",
        "WalletStateInterface",
        "BuyInWalletPolicyInterface",
        "WalletBuyInPolicyInterface",
    ] {
        assert!(
            layer.contains(&format!("pub trait {name} {{")),
            "{name}:\n{layer}"
        );
    }
    golden("buyin.domain.rs", &layer);
    // Each carries the allowance its handlers' `Failed` needs, so no crate
    // implementing it does at the trait's include site.
    assert_eq!(
        layer
            .matches("#[allow(clippy::result_large_err)]\npub trait ")
            .count(),
        4,
        "{layer}"
    );
}

/// A user file imports nothing from the runtime directly: its context and
/// error types come through `crate::domain`, and no code line names the
/// runtime's crates, nor its markup.
#[test]
fn test_the_users_files_import_nothing_from_the_runtime_directly() {
    for f in buyin() {
        let code: Vec<&str> = f
            .source
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect();
        for line in &code {
            for runtime in [
                "angzarr_embedded",
                "angzarr_",
                "#[on_",
                "#[handles",
                "#[policy",
            ] {
                assert!(
                    !line.contains(runtime),
                    "{} names {runtime}: {line}",
                    f.path
                );
            }
            if let Some(path) = line.trim_start().strip_prefix("use ") {
                assert!(path.starts_with("crate::"), "{} imports {path}", f.path);
            }
        }
    }
}

/// An aggregate's snapshot methods, which its interface offers and does
/// not require: reported optional where its scaffold's configuration
/// selects neither.
fn snapshots() -> Vec<String> {
    ["snapshot", "restore"].map(String::from).to_vec()
}

/// `text` without whitespace, and without the trailing comma rustfmt puts
/// before a closing parenthesis: signatures compare whatever way they wrap.
fn flat(text: &str) -> String {
    text.replace([' ', '\n'], "").replace(",)", ")")
}

/// The interface of the policy `name`, from its trait to the next.
fn policy_trait<'a>(source: &'a str, name: &str) -> &'a str {
    let start = source
        .find(&format!("pub trait {name}Interface {{"))
        .unwrap_or_else(|| panic!("no trait {name}:\n{source}"));
    let rest = &source[start..];
    &rest[..rest.find("\n}\n").map_or(rest.len(), |end| end + 3)]
}

#[test]
fn test_every_implementation_body_is_todo_and_every_parameter_unused() {
    for f in buyin().iter().filter(|f| f.path.ends_with("_impl.rs")) {
        let file = syn::parse_file(&f.source).unwrap();
        let imp = file
            .items
            .iter()
            .find_map(|i| match i {
                syn::Item::Impl(imp) if imp.trait_.is_some() => Some(imp),
                _ => None,
            })
            .expect("the trait impl");
        for f in methods(imp) {
            assert_eq!(text(&f.block), "{todo!()}");
            for input in &f.sig.inputs {
                if let syn::FnArg::Typed(t) = input {
                    assert!(text(&t.pat).starts_with('_'), "{}", text(&f.sig));
                }
            }
        }
    }
}

#[test]
fn test_an_interface_takes_a_fact_as_fact_and_a_command_as_cmd() {
    let source = buyin_layer();
    assert!(
        flat(&source).contains(&flat(
            "fn hold_lost(&self, fact: &HoldLost, ctx: &Context) -> Result<Vec<BuyInStateEvent>, Bug>;"
        )),
        "a fact fails only with a Bug:\n{source}"
    );
    assert!(
        flat(&source).contains(&flat(
            "fn record_held(&self, cmd: &RecordHeld, ctx: &Context) -> Result<Vec<BuyInStateEvent>, Failed<BuyInRefused>>;"
        )),
        "{source}"
    );
}

/// A policy whose target has a command that can refuse, and whose source
/// takes facts, has ONE required `refused` callback: the command refused
/// (the target's command choice), the refusal (the target's refusal choice,
/// one arm per refusal message its commands name) and the refused answer,
/// answering one of the source's facts or a Bug, never declining
/// (decisions 14.89, 14.99; C-0612).
#[test]
fn test_a_policys_interface_requires_its_one_refusal_callback() {
    let source = buyin_layer();
    let buy_in_wallet = policy_trait(&source, "BuyInWalletPolicy");
    assert!(
        flat(buy_in_wallet).contains(&flat(
            "fn refused(&self, command: WalletStateCommand, refusal: WalletStateRefusal, ctx: &Refused) -> Result<BuyInStateFact, Bug>;"
        )),
        "{buy_in_wallet}"
    );
    assert!(!source.contains("Ok(None)"), "{source}");
    // One callback, not one per command.
    for policy in ["BuyInWalletPolicy", "WalletBuyInPolicy"] {
        let interface = policy_trait(&source, policy);
        assert_eq!(interface.matches("fn refused(").count(), 1, "{interface}");
        assert!(!interface.contains("_refused("), "{interface}");
    }
    let wallet_buy_in = policy_trait(&source, "WalletBuyInPolicy");
    assert!(
        flat(wallet_buy_in).contains(&flat(
            "fn refused(&self, command: BuyInStateCommand, refusal: BuyInStateRefusal, ctx: &Refused) -> Result<WalletStateFact, Bug>;"
        )),
        "{wallet_buy_in}"
    );
}

/// The refusal a policy is handed is a choice with one arm per refusal
/// message its target's commands name, each once (typed only, decision
/// 14.95); a callback matching it is exhaustive, so a missing arm does not
/// compile. A target none of whose commands can refuse has no choice.
#[test]
fn test_the_refusal_choice_has_one_arm_per_refusal_message_of_the_target() {
    let source = buyin_layer();
    let file = syn::parse_file(&source).unwrap();
    let arms = |name: &str| -> Vec<String> {
        file.items
            .iter()
            .find_map(|i| match i {
                syn::Item::Enum(e) if e.ident == name => {
                    Some(e.variants.iter().map(|v| v.ident.to_string()).collect())
                }
                _ => None,
            })
            .unwrap_or_else(|| panic!("no enum {name}:\n{source}"))
    };
    // Deposit refuses DepositRefused, Hold InsufficientFunds.
    assert_eq!(
        arms("WalletStateRefusal"),
        ["DepositRefused", "InsufficientFunds"]
    );
    // StartBuyIn and RecordHeld both refuse BuyInRefused: one arm.
    assert_eq!(arms("BuyInStateRefusal"), ["BuyInRefused"]);
    // A choice built of its types and read from the refused answer.
    assert!(
        flat(&source).contains(&flat(
            "pub fn read(refused: &Refused) -> Result<(WalletStateCommand, WalletStateRefusal), Bug>"
        )),
        "{source}"
    );
}

/// The implementation is created with its one refusal callback, a
/// `todo!()`: it is required, never optional (decisions 14.89, 14.99).
#[test]
fn test_scaffold_writes_the_one_refused_method_of_the_policy() {
    let files = buyin();
    let implementation = file_at(&files, "buy_in_wallet_policy_impl.rs");
    let source = &implementation.source;
    assert!(
        flat(source).contains(&flat(
            "fn refused(&self, _command: WalletStateCommand, _refusal: WalletStateRefusal, _ctx: &Refused) -> Result<BuyInStateFact, Bug> { todo!() }"
        )),
        "{source}"
    );
    assert_eq!(source.matches("fn refused(").count(), 1, "{source}");
    assert!(implementation.optional.is_empty());
    assert!(implementation.selected.is_empty());
}

/// A target that cannot refuse needs no path (C-0861): no refusal choice,
/// no callback in the policy's interface, nor `#[on_refused]` in its
/// adapter; a target that can has both.
#[test]
fn test_a_target_that_cannot_refuse_has_no_refusal_choice_or_callback() {
    const UNREFUSED: &[&str] = &["unrefused/v1/unrefused.proto"];
    let source = layer(UNREFUSED, "unrefused.v1");
    assert!(
        source.contains("pub trait OrderToInventoryPolicyInterface {"),
        "{source}"
    );
    assert!(!source.contains("StateRefusal"), "{source}");
    assert!(!source.contains("fn refused("), "{source}");
    assert!(!source.contains("fn read("), "{source}");
    let adapters = adapters(&descriptors(UNREFUSED), UNREFUSED, &[]).unwrap();
    assert!(!adapters.contains("on_refused"), "{adapters}");
    assert!(!adapters.contains("Refused"), "{adapters}");
    let files = scaffold(&descriptors(UNREFUSED), UNREFUSED).unwrap();
    let implementation = &file_at(&files, "order_to_inventory_policy_impl.rs").source;
    assert!(!implementation.contains("fn refused("), "{implementation}");
    // The contrast: the buy-in policies' targets refuse.
    let buyin = adapters_of_buyin();
    assert_eq!(buyin.matches("#[on_refused]").count(), 2, "{buyin}");
}

fn adapters_of_buyin() -> String {
    adapters(&descriptors(BUYIN), BUYIN, &[]).unwrap()
}

// --------------------------------------------------------------- blackjack

#[test]
fn test_an_interface_of_one_file_finds_its_policies_owners_and_events_elsewhere() {
    // policies.proto alone is scaffolded: the table and the player owning
    // the policies' domains, and the events they react to, are declared in
    // other files, as the domain layer's interfaces are.
    const POLICIES: &str = "io/angzarr/examples/v1/policies.proto";
    let files = scaffold(&descriptors(BLACKJACK), &[POLICIES]).unwrap();
    file_at(&files, "player_table_policy_impl.rs");
    let source = layer(BLACKJACK, "io.angzarr.examples.v1");
    let player_table = &source[source.find("pub trait PlayerTablePolicyInterface").unwrap()..];
    assert!(player_table.contains("fn move_decided("), "{player_table}");
    // What it sends the table: its outputs, commands and facts alike
    // (decision 14.69).
    assert!(player_table.contains("TableStateOutput"), "{player_table}");
}

#[test]
fn test_a_decision_returns_a_list_of_its_events() {
    const TABLE: &str = "io/angzarr/examples/v1/table.proto";
    let source = layer(BLACKJACK, "io.angzarr.examples.v1");
    let table = &source[source.find("pub trait TableStateInterface").unwrap()..];
    // Decision 14.75: every handling returns a list of the table's events
    // (C-0797), and the table refuses with the choice of its command's refusal types.
    for expected in [
        "        cmd: &PlaceBet,\n        ctx: &Context,\n    ) -> Result<Vec<TableStateEvent>, Failed<PlaceBetRefuses>>;",
        "        cmd: &Hit,\n        ctx: &Context,\n    ) -> Result<Vec<TableStateEvent>, Failed<HitRefuses>>;",
        "        cmd: &CreateTable,\n        ctx: &Context,\n    ) -> Result<Vec<TableStateEvent>, Failed<CreateTableRefuses>>;",
        "        cmd: &DealRound,\n        ctx: &Context,\n    ) -> Result<Vec<TableStateEvent>, Failed<DealRoundRefuses>>;",
        "        cmd: &ConfirmSeat,\n        ctx: &Context,\n    ) -> Result<Vec<TableStateEvent>, Failed<SeatNotConfirmable>>;",
        // ReleaseSeat is a fact (ruling 9, decision 14.69): recorded, never
        // refused.
        "fn release_seat(&self, fact: &ReleaseSeat, ctx: &Context) -> Result<Vec<TableStateEvent>, Bug>;",
    ] {
        assert!(
            flat(table).contains(&flat(expected)),
            "{expected}:\n{table}"
        );
    }
    let files = scaffold(&descriptors(BLACKJACK), &[TABLE]).unwrap();
    let state = &file_at(&files, "table_state_impl.rs").source;
    assert!(
        state.contains("pub state: crate::wire::TableState,"),
        "{state}"
    );
}

// ------------------------------------------------------------- adapters

#[test]
fn test_adapters_match_the_golden_file() {
    let source = adapters(&descriptors(BUYIN), BUYIN, &[]).unwrap();
    golden("buyin.adapters.rs", &source);
}

#[test]
fn test_adapters_name_each_policy_by_its_domains() {
    let source = adapters(&descriptors(BUYIN), BUYIN, &[]).unwrap();
    for name in [
        "#[policy(name = \"policy-buyin-wallet\", source = BuyInState, target = WalletState)]",
        "#[policy(name = \"policy-wallet-buyin\", source = WalletState, target = BuyInState)]",
    ] {
        assert!(source.contains(name), "{name}:\n{source}");
    }
}

/// A policy whose target has a command that can refuse, and whose source
/// takes facts, has ONE refusal callback, delegating to its interface's
/// required `refused`: it reads the command refused and the refusal as the
/// target's choices from the refused answer and hands them on, and the
/// markup always gets a fact: the generated policy never declines
/// (decisions 14.89, 14.99).
#[test]
fn test_adapters_wire_the_one_translating_refusal_callback() {
    let source = adapters(&descriptors(BUYIN), BUYIN, &[]).unwrap();
    let file = parse_adapters(&source);
    let imp = inherent_impl(&file, "BuyInWalletPolicy");
    assert_eq!(
        markers(imp),
        s(&[("on_event", "BuyInStarted"), ("on_refused", "")])
    );
    let callback = methods(imp)[1];
    assert_eq!(
        callback
            .sig
            .inputs
            .iter()
            .skip(1)
            .map(|input| match input {
                syn::FnArg::Typed(t) => text(&t.ty),
                syn::FnArg::Receiver(_) => String::new(),
            })
            .collect::<Vec<_>>(),
        ["&Refused"]
    );
    assert_eq!(
        text(&callback.sig.output),
        "->::core::result::Result<BuyInStateFact,Bug>"
    );
    assert_eq!(
        text(&callback.block),
        "{let(command,refusal)=WalletStateRefusal::read(refused)?;\
         crate::domain::BuyInWalletPolicyInterface::refused(&self.0,command,refusal,refused,)}"
    );
    // The wallet's policy to the buy-in: its own one callback.
    let other = inherent_impl(&file, "WalletBuyInPolicy");
    assert_eq!(
        markers(other),
        s(&[("on_event", "Held"), ("on_refused", "")])
    );
    assert!(
        text(&methods(other)[1].block).contains("BuyInStateRefusal::read(refused)"),
        "{}",
        text(&methods(other)[1].block)
    );
}

const KINDS: &[&str] = &["kinds/v1/kinds.proto"];

/// The audit domain's commands refuse two types between them (Audit:
/// PostRefused and Unreasoned): the policy's refusal is the choice of
/// exactly those, in command order, whatever command refused.
#[test]
fn test_a_refused_method_takes_the_choice_of_the_targets_refusals() {
    let source = layer(KINDS, "kinds.v1");
    assert!(
        flat(&source).contains(&flat(
            "fn refused(&self, command: AuditStateCommand, refusal: AuditStateRefusal, ctx: &Refused) -> Result<LedgerStateFact, Bug>;"
        )),
        "{source}"
    );
    let file = syn::parse_file(&source).unwrap();
    let arms: Vec<String> = file
        .items
        .iter()
        .find_map(|i| match i {
            syn::Item::Enum(e) if e.ident == "AuditStateRefusal" => {
                Some(e.variants.iter().map(|v| v.ident.to_string()).collect())
            }
            _ => None,
        })
        .expect("the audit state's refusal choice");
    assert_eq!(arms, ["PostRefused", "Unreasoned"]);
    let files = scaffold(&descriptors(KINDS), KINDS).unwrap();
    let source = &file_at(&files, "ledger_audit_policy_impl.rs").source;
    assert!(source.contains("_refusal: AuditStateRefusal,"), "{source}");
}

/// A fact handler's adapter hands the interface its context too: the
/// router's `FactEmit`, its one failure a Bug, never a refusal.
#[test]
fn test_adapters_forward_a_fact_handlers_context() {
    let source = adapters(&descriptors(BUYIN), BUYIN, &[]).unwrap();
    let file = parse_adapters(&source);
    let buy_in = inherent_impl(&file, "BuyInState");
    let fact = methods(buy_in)
        .into_iter()
        .find(|m| m.sig.ident == "hold_lost")
        .expect("the HoldLost handler");
    assert_eq!(
        text(&fact.block),
        "{crate::domain::BuyInStateInterface::hold_lost(&self.0,&fact,ctx).map(Self::recorded).map_err(::angzarr_embedded::markup::Bug::from)}"
    );
    assert_eq!(
        text(&fact.sig.output),
        "->Result<FactEmit<Self>,::angzarr_embedded::markup::Bug>"
    );
}

/// The attribute and the signature follow the message's option (C-0648):
/// a command is `#[handles_cmd]`, returning the markup's `Emit` and failing
/// with its refusals (`Refuses<Self, Cmd>`, in a `Failed` so it can still be
/// a bug) or, refusing nothing, with `Failed<Unrefusable>`; a fact is
/// `#[handles_fact]`, returning the router's `FactEmit` and failing only
/// with its `Bug`, with no `refuses` or `starts` it does not declare.
#[test]
fn test_each_handler_is_marked_and_typed_by_its_messages_option() {
    let source = adapters(&descriptors(BUYIN), BUYIN, &[]).unwrap();
    let file = parse_adapters(&source);
    let buy_in = inherent_impl(&file, "BuyInState");
    assert_eq!(
        markers(buy_in)
            .into_iter()
            .filter(|(name, _)| name.starts_with("handles_"))
            .collect::<Vec<_>>(),
        s(&[
            ("handles_cmd", "StartBuyIn,starts,refuses(BuyInRefused)"),
            ("handles_cmd", "RecordHeld,refuses(BuyInRefused)"),
            ("handles_fact", "HoldLost"),
        ])
    );
    assert_eq!(
        signatures(buy_in)[..3],
        [
            "fnstart_buy_in(&self,cmd:StartBuyIn,ctx:&Context,)->Result<Emit<Self>,Failed<Refuses<Self,StartBuyIn>>>",
            "fnrecord_held(&self,cmd:RecordHeld,ctx:&Context,)->Result<Emit<Self>,Failed<Refuses<Self,RecordHeld>>>",
            "fnhold_lost(&self,fact:HoldLost,ctx:&Context,)->Result<FactEmit<Self>,::angzarr_embedded::markup::Bug>",
        ]
    );
    // A command that refuses nothing cannot refuse: it fails with
    // `Failed<Unrefusable>` and lists no `refuses`.
    let kinds = adapters(&descriptors(KINDS), KINDS, &[]).unwrap();
    let kinds = parse_adapters(&kinds);
    let ledger = inherent_impl(&kinds, "LedgerState");
    let open = methods(ledger)
        .into_iter()
        .find(|m| m.sig.ident == "open")
        .expect("the Open handler");
    assert_eq!(
        text(&open.sig.output),
        "->Result<Emit<Self>,Failed<Unrefusable>>"
    );
    assert_eq!(
        attribute(&open.attrs[0]),
        ("handles_cmd".to_string(), "Open".to_string())
    );
}

/// A command handler's adapter hands the interface its context; a policy
/// reaction's hands it its context and answers its Result.
#[test]
fn test_adapters_forward_each_handlers_context() {
    let source = adapters(&descriptors(BUYIN), BUYIN, &[]).unwrap();
    let file = parse_adapters(&source);
    let wallet = inherent_impl(&file, "WalletState");
    assert_eq!(
        text(&methods(wallet)[0].block),
        "{crate::domain::WalletStateInterface::deposit(&self.0,&cmd,ctx).map(Self::emitted::<Failed<Refuses<Self,Deposit>>>).map_err(DepositRefused::with_declared_status).map_err(Failed::widen)}"
    );
    let policy = inherent_impl(&file, "BuyInWalletPolicy");
    let reaction = methods(policy)[0];
    assert_eq!(
        text(&reaction.sig.output),
        "->::core::result::Result<Commands<WalletState>,Bug>"
    );
    assert!(
        text(&reaction.block).starts_with(
            "{letorders=crate::domain::BuyInWalletPolicyInterface::buy_in_started(&self.0,&ctx.source_root().value,event,ctx,)?;"
        ),
        "{}",
        text(&reaction.block)
    );
}

/// A policy reaction takes the source root, the event and its context, and
/// answers its outputs (its target's commands and facts) or its error, a
/// bug.
#[test]
fn test_an_interface_gives_a_policy_reaction_its_context_and_a_result() {
    let source = buyin_layer();
    assert!(
        source.contains(
            "    fn buy_in_started(\n        &self,\n        source: &[u8],\n        event: &BuyInStarted,\n        ctx: &PolicyContext,\n    ) -> Result<Vec<(Vec<u8>, WalletStateOutput)>, Bug>;"
        ),
        "{source}"
    );
}

/// A domain's handlers fail with the generated layer's `Failed`, larger
/// than clippy's `result_large_err` allows; the generated module allows it,
/// so no application's include site need (item S).
#[test]
fn test_adapters_allow_a_domains_large_failure() {
    let source = adapters(&descriptors(BUYIN), BUYIN, &[]).unwrap();
    let file = syn::parse_file(&source).unwrap();
    let module = file
        .items
        .iter()
        .find_map(|i| match i {
            syn::Item::Mod(m) if m.ident == "adapters" => Some(m),
            _ => None,
        })
        .expect("the adapters module");
    assert!(
        module
            .attrs
            .iter()
            .any(|a| text(a) == "#[allow(clippy::result_large_err)]"),
        "{source}"
    );
}

/// A policy a child process serves has no adapter here, named by its name
/// in the application; the domains it joins keep theirs.
#[test]
fn test_adapters_leave_a_child_policy_to_its_process() {
    let all = adapters(&descriptors(BUYIN), BUYIN, &[]).unwrap();
    assert!(all.contains("policy-buyin-wallet"), "{all}");
    let source = adapters(&descriptors(BUYIN), BUYIN, &["policy-buyin-wallet"]).unwrap();
    assert!(!source.contains("policy-buyin-wallet"), "{source}");
    assert!(source.contains("policy-wallet-buyin"), "{source}");
}

/// A domain served by a child process is no aggregate here: a policy
/// names it by its command set (`CommandTarget`, with `Decides` of each
/// command) and its event set (`EventSource`, with `Ingests` of each
/// fact), and no stand-in markup is generated for it.
#[test]
fn test_adapters_name_a_remote_domain_by_its_commands_and_events() {
    let source = adapters(&descriptors(BUYIN), BUYIN, &["buyin"]).unwrap();
    let file = parse_adapters(&source);
    let impls: Vec<&syn::ItemImpl> = file
        .items
        .iter()
        .filter_map(|i| match i {
            syn::Item::Impl(imp) if text(&imp.self_ty) == "BuyInState" => Some(imp),
            _ => None,
        })
        .collect();
    assert!(
        impls.iter().all(|imp| imp.trait_.is_some()),
        "no markup impl of the buy-in: {source}"
    );
    let traits: Vec<String> = impls
        .iter()
        .filter_map(|imp| imp.trait_.as_ref().map(|t| text(&t.0)))
        .collect();
    assert_eq!(
        traits,
        [
            "CommandTarget",
            "Decides<StartBuyIn>",
            "Decides<RecordHeld>",
            "EventSource",
            "Ingests<HoldLost>",
        ]
    );
    let body = |name: &str| {
        impls
            .iter()
            .find(|imp| imp.trait_.as_ref().is_some_and(|t| text(&t.0) == name))
            .map(|imp| imp.items.iter().map(text).collect::<String>())
            .unwrap()
    };
    let target = body("CommandTarget");
    assert!(
        target.contains("constDOMAIN:&'staticstr=\"buyin\";"),
        "{target}"
    );
    assert!(
        target.contains(
            "vec![\"buyin.v1.StartBuyIn\".to_string(),\"buyin.v1.RecordHeld\".to_string()]"
        ),
        "{target}"
    );
    let events = body("EventSource");
    assert!(
        events.contains("constDOMAIN:&'staticstr=\"buyin\";"),
        "{events}"
    );
    assert!(
        events.contains(
            "vec![\"buyin.v1.BuyInStarted\".to_string(),\"buyin.v1.BuyInHeld\".to_string(),\"buyin.v1.BuyInFailed\".to_string()]"
        ),
        "{events}"
    );
    assert!(!source.contains("crate::buy_in_state_impl::"), "{source}");
    assert!(!source.contains("unreachable!"), "{source}");
}

/// The markup the adapters import, by name.
fn markup_imports(source: &str) -> Vec<String> {
    let file = parse_adapters(source);
    let tree = file
        .items
        .iter()
        .find_map(|i| match i {
            syn::Item::Use(u) if text(u).contains("markup::") => Some(text(&u.tree)),
            _ => None,
        })
        .expect("a markup import");
    let (_, names) = tree.rsplit_once('{').unwrap();
    names
        .trim_end_matches('}')
        .split(',')
        .map(str::to_string)
        .collect()
}

/// A remote domain's import takes `Decides` only when a command of its own
/// reaches it, `Ingests` only when a fact does: the kinds audit, commands
/// alone, imports `Decides` and no `Ingests`; the buy-in, a fact as well,
/// both.
#[test]
fn test_adapters_import_a_remote_domains_markup_by_its_inputs() {
    let audit = adapters(&descriptors(KINDS), KINDS, &["audit"]).unwrap();
    let imports = markup_imports(&audit);
    assert!(imports.iter().any(|m| m == "Decides"), "{imports:?}");
    assert!(!imports.iter().any(|m| m == "Ingests"), "{imports:?}");
    let buyin = adapters(&descriptors(BUYIN), BUYIN, &["buyin"]).unwrap();
    let imports = markup_imports(&buyin);
    assert!(imports.iter().any(|m| m == "Decides"), "{imports:?}");
    assert!(imports.iter().any(|m| m == "Ingests"), "{imports:?}");
}

// ------------------------------------------------------------- --missing

/// The wallet's implementation as a user has it: Hold and Held absent (the
/// proto gained them later), a method and a doc comment of the user's own,
/// and the decision of a command the proto no longer declares.
const USER_WALLET_IMPL: &str = r#"//! The wallet, mine.

use crate::domain::*;

#[derive(Clone, Debug, Default)]
pub struct WalletStateImpl {
    pub state: crate::wire::WalletState,
}

impl WalletStateInterface for WalletStateImpl {
    /// The user's own doc, which `--missing` must keep byte for byte.
    fn deposit(&self, cmd: &Deposit, _ctx: &Context) -> Result<Vec<WalletStateEvent>, Failed<DepositRefused>> {
        Ok(vec![Deposited { cents: cmd.cents }.into()]) // USER-WRITTEN
    }

    fn record_buy_in_refused(&self, _fact: &RecordBuyInRefused, _ctx: &Context) -> Result<Vec<WalletStateEvent>, Bug> {
        Ok(Vec::new())
    }

    fn apply_buy_in_refusal_recorded(&mut self, _event: &BuyInRefusalRecorded) {}

    fn withdraw(&self, cmd: &Withdraw, _ctx: &Context) -> Result<Deposited, Failed<Unrefusable>> {
        todo!()
    }

    fn apply_deposited(&mut self, event: &Deposited) {
        self.state.balance += event.cents;
    }
}
"#;

#[test]
fn test_complete_adds_only_the_absent_required_methods_to_the_implementation() {
    let files = buyin();
    let completion = complete(
        file_at(&files, "wallet_state_impl.rs"),
        USER_WALLET_IMPL,
        Existing::Complete,
    )
    .unwrap();
    let expected = USER_WALLET_IMPL.replace(
        "        self.state.balance += event.cents;\n    }\n}",
        "        self.state.balance += event.cents;\n    }\n\n    \
         fn hold(\n        &self,\n        _cmd: &Hold,\n        _ctx: &Context,\n    ) -> Result<Vec<WalletStateEvent>, Failed<InsufficientFunds>> {\n        todo!()\n    }\n\n    \
         fn apply_held(&mut self, _event: &Held) {\n        todo!()\n    }\n}",
    );
    assert_eq!(completion.completed, Some(expected));
    assert_eq!(
        completion.notes,
        Notes {
            obsolete: vec!["withdraw".to_string()],
            optional: snapshots(),
            ..Notes::default()
        }
    );
}

/// Kept, a file is never written: what it lacks is reported, and what it
/// has for a message the protos no longer declare.
#[test]
fn test_complete_of_a_kept_file_reports_what_it_lacks_and_what_is_obsolete() {
    let files = buyin();
    let completion = complete(
        file_at(&files, "wallet_state_impl.rs"),
        USER_WALLET_IMPL,
        Existing::Keep,
    )
    .unwrap();
    assert_eq!(completion.completed, None);
    assert_eq!(
        completion.notes,
        Notes {
            lacks: vec!["hold".to_string(), "apply_held".to_string()],
            obsolete: vec!["withdraw".to_string()],
            optional: snapshots(),
            ..Notes::default()
        }
    );
}

/// A policy's refusal callback is required (decisions 14.89, 14.99): `--missing`
/// adds one an existing implementation lacks, as any required method, and
/// a kept file reports it.
#[test]
fn test_complete_adds_a_missing_refused_method() {
    let plain = buyin();
    let path = "buy_in_wallet_policy_impl.rs";
    let fresh = file_at(&plain, path);
    let start = fresh.source.find("    fn refused(").unwrap();
    let end = start + fresh.source[start..].find("    }\n").unwrap() + "    }\n".len();
    let mine = format!("{}{}", &fresh.source[..start], &fresh.source[end..]);
    let kept = complete(fresh, &mine, Existing::Keep).unwrap();
    assert_eq!(kept.completed, None);
    assert_eq!(kept.notes.lacks, ["refused"]);
    assert!(kept.notes.optional.is_empty());
    let completed = complete(fresh, &mine, Existing::Complete)
        .unwrap()
        .completed
        .expect("the required callback is added");
    assert!(completed.contains("fn refused("), "{completed}");
}

#[test]
fn test_complete_refuses_a_command_taken_as_a_fact() {
    let files = buyin();
    let fresh = file_at(&files, "buy_in_state_impl.rs");
    let as_fact = fresh
        .source
        .replace("_cmd: &RecordHeld", "_fact: &RecordHeld");
    assert_ne!(as_fact, fresh.source);
    assert_eq!(
        complete(fresh, &as_fact, Existing::Keep),
        Err(CodegenError::Use(UseError::InputMismatch {
            path: "buy_in_state_impl.rs".to_string(),
            message: "buyin.v1.RecordHeld".to_string(),
            expected: "cmd".to_string(),
            found: "fact".to_string(),
        }))
    );
}

#[test]
fn test_complete_refuses_a_file_without_the_components_impl() {
    let files = buyin();
    assert_eq!(
        complete(
            file_at(&files, "wallet_state_impl.rs"),
            "//! Not the wallet any more.\n",
            Existing::Keep
        ),
        Err(CodegenError::Use(UseError::ScaffoldImplNotFound {
            path: "wallet_state_impl.rs".to_string(),
            component: "buyin.v1.WalletState".to_string(),
        }))
    );
}

#[test]
fn test_complete_inserts_before_a_closing_brace_that_ends_a_line() {
    let files = buyin();
    let user = USER_WALLET_IMPL.replace(
        "        self.state.balance += event.cents;\n    }\n}\n",
        "        self.state.balance += event.cents;\n    } }\n",
    );
    assert_ne!(user, USER_WALLET_IMPL);
    let completed = complete(
        file_at(&files, "wallet_state_impl.rs"),
        &user,
        Existing::Complete,
    )
    .unwrap()
    .completed
    .expect("hold and apply_held are absent");
    assert!(
        completed.ends_with(
            "    } \n\n    fn hold(\n        &self,\n        _cmd: &Hold,\n        _ctx: &Context,\n    ) -> Result<Vec<WalletStateEvent>, Failed<InsufficientFunds>> {\n        todo!()\n    }\n\n    \
             fn apply_held(&mut self, _event: &Held) {\n        todo!()\n    }\n}\n"
        ),
        "{completed}"
    );
}

#[test]
fn test_complete_takes_a_method_naming_its_event_otherwise_as_present() {
    let files = buyin();
    let fresh = file_at(&files, "wallet_state_impl.rs");
    let renamed = fresh
        .source
        .replace("_event: &Deposited", "deposited: &Deposited");
    assert_ne!(renamed, fresh.source);
    assert_eq!(
        complete(fresh, &renamed, Existing::Complete)
            .unwrap()
            .completed,
        None
    );
}

/// A manifest is never completed: its pins that are not this CLI's are
/// reported (item S Q8).
#[test]
fn test_complete_of_a_manifest_reports_its_pins_that_drift() {
    let fresh = ScaffoldFile {
        component: String::new(),
        path: "Cargo.toml".to_string(),
        source: "[dependencies]\nangzarr-embedded = { git = \"g\", rev = \"new\" }\nother = { git = \"o\", rev = \"x\" }\n".to_string(),
        owner: Owner::User,
        optional: Vec::new(),
        selected: Vec::new(),
    };
    let mine = "[dependencies]\nangzarr-embedded = { git = \"g\", rev = \"old\" }\nother = { path = \"../o\" }\n";
    let completion = complete(&fresh, mine, Existing::Complete).unwrap();
    assert_eq!(completion.completed, None);
    assert_eq!(
        completion.notes.drift,
        [angzarr_codegen::PinDrift {
            dependency: "angzarr-embedded".to_string(),
            pinned: "old".to_string(),
            cli: "new".to_string(),
        }]
    );
    assert_eq!(
        complete(&fresh, &fresh.source, Existing::Keep)
            .unwrap()
            .notes,
        Notes::default()
    );
}
