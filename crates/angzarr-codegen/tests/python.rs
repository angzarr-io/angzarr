//! The Python scaffold: one domain's uv project, the user's logic
//! interface and implementation written once over the domain's plain types,
//! the plain types, the dispatch and the harness glue the CLI's. Pinned by golden files (`tests/golden/python/player`), the
//! blackjack player of spec decision 14.38.

mod common;

use angzarr_codegen::{
    CodegenError, Existing, Notes, Owner, ProtoError, ProtoInput, PythonApp, ScaffoldFile,
    UseError, complete, python_reroots, reroot_python, scaffold_python,
};
use common::*;

use angzarr_test_dir::TestDir;

const GOLDEN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/python/player");

fn player_app() -> PythonApp {
    PythonApp {
        domain: "player".to_string(),
        protos: ProtoInput::Git {
            url: "https://github.com/angzarr-io/angzarr-project".to_string(),
            rev: "ef975bcf6fd6a0207476667d18ddc7bb29a10ab4".to_string(),
        },
        rules_version: "0.1.0".to_string(),
    }
}

/// The player's project, from player.proto alone: the rest of the
/// blackjack protos are in the descriptor set (a policy and a projector
/// name the player's events), not scaffolded.
fn player() -> Vec<ScaffoldFile> {
    scaffold_python(&descriptors(BLACKJACK), &[PLAYER], &player_app()).unwrap()
}

fn file<'a>(files: &'a [ScaffoldFile], path: &str) -> &'a ScaffoldFile {
    files
        .iter()
        .find(|f| f.path == path)
        .unwrap_or_else(|| panic!("no {path}"))
}

const IMPL: &str = "src/angzarr_player/logic_impl.py";
const INTERFACE: &str = "src/angzarr_player/_gen/logic.py";

/// The package's root holds the user's files alone, written once; every
/// generated module is under `_gen` (the one path a gate excludes), and
/// buf's message types beside them are its own; the modules an earlier
/// scaffold generated beside the user's are reported, never written
/// (item S).
#[test]
fn test_scaffold_python_writes_the_projects_files_each_with_its_owner() {
    let mut placed: Vec<(String, Owner)> =
        player().into_iter().map(|f| (f.path, f.owner)).collect();
    placed.sort_by(|a, b| a.0.cmp(&b.0));
    let gen_dir = "src/angzarr_player/_gen";
    let v1 = format!("{gen_dir}/io/angzarr/examples/v1");
    let mut expected: Vec<(String, Owner)> = vec![
        ("buf.gen.yaml".to_string(), Owner::Generated),
        ("pyproject.toml".to_string(), Owner::User),
        ("src/angzarr_player/__init__.py".to_string(), Owner::User),
        ("src/angzarr_player/__main__.py".to_string(), Owner::User),
        (IMPL.to_string(), Owner::User),
        ("tests/__init__.py".to_string(), Owner::User),
    ];
    for generated in [
        "__init__.py",
        "io/__init__.py",
        "io/angzarr/__init__.py",
        "io/angzarr/examples/__init__.py",
        "io/angzarr/examples/v1/__init__.py",
        "io/angzarr/examples/v1/player_state_angzarr.py",
        "_descriptor.py",
        "_version.py",
        "domain.py",
        "logic.py",
        "main.py",
    ] {
        expected.push((format!("{gen_dir}/{generated}"), Owner::Generated));
    }
    for proto in ["cards", "player"] {
        for ext in ["py", "pyi"] {
            expected.push((format!("{v1}/{proto}_pb2.{ext}"), Owner::External));
        }
    }
    for moved in ["domain", "logic", "main", "_version", "_descriptor"] {
        expected.push((format!("src/angzarr_player/{moved}.py"), Owner::Obsolete));
    }
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(placed, expected);
}

/// Each file of the player's project against its golden file;
/// `ANGZARR_BLESS=1` rewrites the golden files instead, as for the Rust
/// scaffold's.
#[test]
fn test_scaffold_python_matches_the_golden_project() {
    for f in player()
        .iter()
        .filter(|f| matches!(f.owner, Owner::User | Owner::Generated))
    {
        let path = format!("{GOLDEN}/{}", f.path);
        if std::env::var_os("ANGZARR_BLESS").is_some() {
            if let Some(dir) = std::path::Path::new(&path).parent() {
                std::fs::create_dir_all(dir).unwrap();
            }
            std::fs::write(&path, &f.source).unwrap();
            continue;
        }
        let golden =
            std::fs::read_to_string(&path).unwrap_or_else(|_| panic!("no golden {}", f.path));
        assert_eq!(f.source, golden, "{}", f.path);
    }
}

/// Every generated file opens with the generated marker (after the licence
/// line), and imports nothing of the user's: inside `_gen` every import of
/// the project's own is relative, and the logic is handed in (item S Q1).
#[test]
fn test_generated_files_carry_the_marker_and_import_no_user_module() {
    for f in player().iter().filter(|f| f.owner == Owner::Generated) {
        assert!(angzarr_codegen::is_generated(&f.source), "{}", f.path);
        if !f.path.ends_with(".py") {
            continue;
        }
        for import in f
            .source
            .lines()
            .filter(|l| l.starts_with("from ") || l.starts_with("import "))
        {
            assert!(!import.contains("angzarr_player"), "{}: {import}", f.path);
            assert!(!import.contains("logic_impl"), "{}: {import}", f.path);
        }
    }
}

/// The child's host answers `Describe` with the generated descriptor,
/// unchanged (decision 14.42): `_gen/main.py` registers the domain with
/// `_descriptor.DESCRIPTOR`, over the logic the user's `__main__.py` builds
/// and hands it.
#[test]
fn test_main_py_registers_the_logic_it_is_handed_with_its_generated_descriptor() {
    let files = player();
    let main = &file(&files, "src/angzarr_player/_gen/main.py").source;
    for expected in [
        "from ._descriptor import DESCRIPTOR\n",
        "from .logic import PlayerLogic\n",
        "new_player_state_dispatch(player_logic), DESCRIPTOR",
        "def run(player_logic: PlayerLogic) -> None:",
    ] {
        assert!(main.contains(expected), "{expected}:\n{main}");
    }
    let dunder = &file(&files, "src/angzarr_player/__main__.py").source;
    for expected in [
        "from ._gen.main import run\n",
        "from .logic_impl import PlayerLogicImpl\n",
        "    run(PlayerLogicImpl())\n",
    ] {
        assert!(dunder.contains(expected), "{expected}:\n{dunder}");
    }
    let pyproject = &file(&files, "pyproject.toml").source;
    assert!(
        pyproject.contains("angzarr-player = \"angzarr_player.__main__:main\""),
        "{pyproject}"
    );
}

/// The interface is an ABC of the required methods, so an implementation
/// lacking one fails to instantiate. A domain has no optional methods: a
/// refusal of a command its policies send is theirs to translate, so the
/// domain is handed no pass-through refusal, and the interface declares
/// no reference protocol of refusal callbacks.
#[test]
fn test_the_interface_is_an_abc_of_the_required_methods_alone() {
    let files = player();
    let logic = &file(&files, INTERFACE).source;
    let abc = &logic[logic.find("class PlayerLogic(ABC):").expect("the ABC")..];
    assert!(
        abc.contains("    @abstractmethod\n    def decide_move("),
        "{abc}"
    );
    assert!(
        abc.contains("    @abstractmethod\n    def apply_move_decided("),
        "{abc}"
    );
    for gone in ["on_refused_", "Protocol", "PlayerRefusals"] {
        assert!(!logic.contains(gone), "{gone}:\n{logic}");
    }
    let implementation = &file(&files, IMPL).source;
    assert!(
        implementation.contains("class PlayerLogicImpl(PlayerLogic):"),
        "{implementation}"
    );
    for gone in ["on_refused_", "PlayerRefusals", "optional"] {
        assert!(!implementation.contains(gone), "{gone}:\n{implementation}");
    }
    assert!(file(&files, IMPL).optional.is_empty());
}

#[test]
fn test_scaffold_python_pins_the_client_by_the_one_constant() {
    let files = player();
    let pyproject = &file(&files, "pyproject.toml").source;
    assert!(
        pyproject.contains(&format!(
            "angzarr-client = {{ git = \"{}\", rev = \"{}\" }}",
            angzarr_codegen::CLIENT_PYTHON_GIT,
            angzarr_codegen::CLIENT_PYTHON_REV
        )),
        "{pyproject}"
    );
}

#[test]
fn test_the_users_files_name_nothing_of_the_harness() {
    // pyproject.toml depends on angzarr-client and protobuf; the user's code
    // names neither.
    for f in player()
        .iter()
        .filter(|f| f.owner == Owner::User && f.path.ends_with(".py"))
    {
        for harness in [
            "angzarr_client",
            "ComponentHost",
            "_az",
            "run_child",
            "_pb2",
            "protobuf",
        ] {
            assert!(
                !f.source.contains(harness),
                "{} names {harness}:\n{}",
                f.path,
                f.source
            );
        }
        // It imports the plain types and its interface from the generated
        // package, relative, and nothing generated from protobuf.
        for import in f
            .source
            .lines()
            .filter(|l| l.starts_with("from ") || l.starts_with("import "))
        {
            assert!(
                import.starts_with("from __future__") || import.starts_with("from ."),
                "{}: {import}",
                f.path
            );
        }
    }
}

/// A decision fails in one class of angzarr-client-python's, re-exported by
/// the domain module: `Bug`, a defect the runtime alerts and dead-letters
/// (ROUTER_REASON_UNHANDLED_HANDLER_ERROR, its key the cause), a diagnostic
/// key and its args, never text. A business "no" is a returned typed
/// refusal, never a raised untyped `Rejection` (decision 14.95).
#[test]
fn test_domain_py_gives_the_logic_its_bug_and_no_untyped_rejection() {
    let files = player();
    let domain = &file(&files, "src/angzarr_player/_gen/domain.py").source;
    for expected in [
        "# - Bug(key, **args):",
        "from angzarr_client.router import Bug as Bug\n",
        "from angzarr_client.router import CommandContext as CommandContext\n",
    ] {
        assert!(domain.contains(expected), "{expected}:\n{domain}");
    }
    for gone in [
        "Rejection",
        "PERMISSION_DENIED",
        "class Bug(",
        "_CodedError",
        "_GrpcCode",
    ] {
        assert!(!domain.contains(gone), "{gone}:\n{domain}");
    }
}

/// A message named as one of the names `domain.py` gives the harness's
/// types (`CommandContext`, `Refused`) is refused, never renamed.
#[test]
fn test_scaffold_python_refuses_a_message_named_as_a_reserved_name() {
    const FILE: &str = "bad/v1/reserved_name.proto";
    let app = PythonApp {
        domain: "things".to_string(),
        ..player_app()
    };
    let refused = scaffold_python(&descriptors(&[FILE]), &[FILE], &app);
    assert!(
        matches!(&refused, Err(CodegenError::Proto(ProtoError::ReservedName { message, name }))
            if message == "bad.v1.Context" && name == "Context"),
        "{refused:?}"
    );
}

/// `Bug`, the class `domain.py` re-exports for a defect, is reserved too.
#[test]
fn test_scaffold_python_refuses_a_message_named_bug() {
    const FILE: &str = "bad/v1/reserved_bug.proto";
    let app = PythonApp {
        domain: "things".to_string(),
        ..player_app()
    };
    let refused = scaffold_python(&descriptors(&[FILE]), &[FILE], &app);
    assert!(
        matches!(&refused, Err(CodegenError::Proto(ProtoError::ReservedName { message, name }))
            if message == "bad.v1.Bug" && name == "Bug"),
        "{refused:?}"
    );
}

/// The dispatch hands each decision the harness's context of the command;
/// DecideMove refuses nothing, so its events go out as the book.
#[test]
fn test_the_dispatch_hands_each_decision_its_context() {
    let files = player();
    let dispatch = files
        .iter()
        .find(|f| f.path.ends_with("_angzarr.py"))
        .expect("the dispatch");
    assert!(
        dispatch
            .source
            .contains("return _book(logic.decide_move(cmd, _to_player_state(state), cctx))"),
        "{}",
        dispatch.source
    );
}

#[test]
fn test_scaffold_python_refuses_a_domain_the_protos_do_not_declare() {
    let app = PythonApp {
        domain: "nowhere".to_string(),
        ..player_app()
    };
    assert_eq!(
        scaffold_python(&descriptors(BLACKJACK), &[PLAYER], &app),
        Err(CodegenError::Use(UseError::PythonNoDomain {
            domain: "nowhere".to_string()
        }))
    );
}

// ------------------------------------------------------------- --missing

/// The implementation as a user has it: the decision written, the fold not
/// there yet, a handler of a message the protos no longer declare, and a
/// helper of its own.
const USER_IMPL: &str = r#""""The player bot."""

from __future__ import annotations

from ._gen.domain import (
    DecideMove,
    MoveAction,
    MoveDecided,
    PlayerState,
)
from ._gen.logic import PlayerLogic


class PlayerLogicImpl(PlayerLogic):
    def decide_move(self, cmd: DecideMove, state: PlayerState) -> list[MoveDecided]:
        action = MoveAction.HIT if cmd.total < 17 else MoveAction.STAND
        return [MoveDecided(table_root=cmd.table_root, round=cmd.round, seat=cmd.seat, action=action)]  # USER-WRITTEN

    def record_move_refused(self, fact: RecordMoveRefused, state: PlayerState) -> list[MoveRefused]:
        return []

    def apply_move_refused(self, state: PlayerState, event: MoveRefused) -> PlayerState:
        return state

    def apply_move_undone(self, state: PlayerState, event: MoveUndone) -> PlayerState:
        return state

    def score(self, hand: Hand) -> int:
        return 0


def helper() -> int:
    return 1
"#;

/// `--missing` adds the required methods the class lacks at its end, in
/// the scaffold's order, and nothing of the user's text changes.
#[test]
fn test_complete_adds_only_the_absent_required_methods_to_the_class() {
    let fresh = file(&player(), IMPL).clone();
    let completion = complete(&fresh, USER_IMPL, Existing::Complete).unwrap();
    let fold = "\n    def apply_move_decided(self, state: PlayerState, event: MoveDecided) \
                -> PlayerState:\n        raise NotImplementedError  # TODO: fold MoveDecided into the state\n";
    assert!(fresh.source.contains(fold), "{}", fresh.source);
    let expected = USER_IMPL.replace("        return 0\n", &format!("        return 0\n{fold}"));
    assert_eq!(completion.completed, Some(expected));
    assert_eq!(completion.notes.lacks, Vec::<String>::new());
    // The user's helper is not the scaffold's; a fold of a message the
    // protos no longer declare is.
    assert_eq!(completion.notes.obsolete, ["apply_move_undone"]);
    // A domain has no optional method to report.
    assert_eq!(completion.notes.optional, Vec::<String>::new());
}

/// Kept, the file is never written: what it lacks is reported.
#[test]
fn test_complete_of_a_kept_class_reports_what_it_lacks() {
    let fresh = file(&player(), IMPL).clone();
    let completion = complete(&fresh, USER_IMPL, Existing::Keep).unwrap();
    assert_eq!(completion.completed, None);
    assert_eq!(completion.notes.lacks, ["apply_move_decided"]);
}

#[test]
fn test_complete_of_a_complete_python_file_changes_nothing() {
    let fresh = file(&player(), IMPL).clone();
    let completion = complete(&fresh, &fresh.source, Existing::Complete).unwrap();
    assert_eq!(completion.completed, None);
    assert_eq!(completion.notes, Notes::default());
}

#[test]
fn test_complete_refuses_a_command_taken_as_a_fact() {
    let fresh = file(&player(), IMPL).clone();
    let as_fact = fresh.source.replace("cmd: DecideMove", "fact: DecideMove");
    assert_ne!(as_fact, fresh.source);
    assert_eq!(
        complete(&fresh, &as_fact, Existing::Keep),
        Err(CodegenError::Use(UseError::InputMismatch {
            path: IMPL.to_string(),
            message: "io.angzarr.examples.v1.DecideMove".to_string(),
            expected: "cmd".to_string(),
            found: "fact".to_string(),
        }))
    );
}

#[test]
fn test_complete_refuses_a_python_file_without_the_class() {
    let fresh = file(&player(), IMPL).clone();
    assert_eq!(
        complete(
            &fresh,
            "\"\"\"Not the player any more.\"\"\"\n",
            Existing::Keep
        ),
        Err(CodegenError::Use(UseError::ScaffoldImplNotFound {
            path: IMPL.to_string(),
            component: "io.angzarr.examples.v1.PlayerState".to_string(),
        }))
    );
}

// --------------------------------------------------------------- reroot

/// buf's message types are headed with the generated marker, angzarr's own
/// imports taken from `angzarr_client.proto`, and the project's made
/// relative within `_gen`: from the importing module's own package.
#[test]
fn test_reroot_python_moves_framework_imports_and_makes_the_projects_relative() {
    let rules = python_reroots(&["io.angzarr.examples.v1", "io.angzarr.examples.v2"]);
    let pb2 = "from google.protobuf import descriptor as _descriptor\n\
               from io.angzarr.examples.v1 import cards_pb2 as io_dot_angzarr_dot_examples_dot_v1_dot_cards__pb2\n\
               from io.angzarr.examples.v2 import deck_pb2 as _deck\n\
               from io.angzarr.v1 import options_pb2 as io_dot_angzarr_dot_v1_dot_options__pb2\n\
               import io.angzarr.v1.types_pb2\n\
               import io.angzarr.examples.v1.table_pb2\n\
               x = 'from io.angzarr.v1 import y'\n";
    let rerooted = reroot_python(pb2, &rules, "io.angzarr.examples.v1");
    let (header, rest) = rerooted.split_once('\n').unwrap();
    assert!(angzarr_codegen::is_generated(header), "{header}");
    assert_eq!(
        rest,
        "from google.protobuf import descriptor as _descriptor\n\
         from . import cards_pb2 as io_dot_angzarr_dot_examples_dot_v1_dot_cards__pb2\n\
         from ..v2 import deck_pb2 as _deck\n\
         from angzarr_client.proto.io.angzarr.v1 import options_pb2 as io_dot_angzarr_dot_v1_dot_options__pb2\n\
         import angzarr_client.proto.io.angzarr.v1.types_pb2\n\
         from . import table_pb2\n\
         x = 'from io.angzarr.v1 import y'\n"
    );
    // Headed once: a module rerooted again keeps its one header.
    assert_eq!(
        reroot_python(&rerooted, &rules, "io.angzarr.examples.v1"),
        rerooted
    );
}

#[test]
fn test_the_logic_takes_and_returns_the_domains_plain_types() {
    let files = player();
    let logic = &file(&files, INTERFACE).source;
    assert!(logic.contains("class PlayerLogic(ABC):"), "{logic}");
    assert!(
        logic.contains(
            "    def decide_move(\n        self, cmd: DecideMove, state: PlayerState, ctx: CommandContext\n    ) -> list[MoveDecided]: ..."
        ),
        "a decision: the command, the state and its context in, its events out:\n{logic}"
    );
    assert!(
        logic.contains("from .domain import (\n    CommandContext,\n"),
        "the context comes through the generated domain module:\n{logic}"
    );
    assert!(
        logic.contains("self, state: PlayerState, event: MoveDecided\n    ) -> PlayerState: ..."),
        "a fold: the state and the event in, the next state out:\n{logic}"
    );
    let domain = &file(&files, "src/angzarr_player/_gen/domain.py").source;
    assert!(
        domain.contains("from angzarr_client.router import CommandContext as CommandContext\n"),
        "domain.py re-exports the harness's context:\n{domain}"
    );
    for plain in [
        "class MoveAction(enum.IntEnum):",
        "    DOUBLE_DOWN = 3",
        "@dataclass(frozen=True)\nclass DecideMove:",
        "    dealer_up: Card | None = None",
        "    cards: tuple[Card, ...] = ()",
        "    action: MoveAction = MoveAction.UNSPECIFIED",
    ] {
        assert!(domain.contains(plain), "{plain}:\n{domain}");
    }
}

#[test]
fn test_reroot_python_takes_the_longest_package_a_line_names() {
    let rules = vec![
        ("io".to_string(), "wrong.io".to_string()),
        (
            "io.angzarr.v1".to_string(),
            "angzarr_client.proto.io.angzarr.v1".to_string(),
        ),
    ];
    let rerooted = reroot_python("from io.angzarr.v1 import options_pb2\n", &rules, "");
    assert!(
        rerooted.ends_with("\nfrom angzarr_client.proto.io.angzarr.v1 import options_pb2\n"),
        "{rerooted}"
    );
}

#[test]
fn test_complete_adds_an_absent_method_that_is_not_the_classes_last() {
    let fresh = file(&player(), IMPL).clone();
    let decide = "    def decide_move(\n        self, cmd: DecideMove, state: PlayerState, ctx: CommandContext\n    ) \
                  -> list[MoveDecided]:\n        raise NotImplementedError  # TODO: decide DecideMove\n";
    let user = fresh.source.replace(&format!("{decide}\n"), "");
    assert_ne!(user, fresh.source);
    let completed = complete(&fresh, &user, Existing::Complete)
        .unwrap()
        .completed
        .expect("decide_move is absent");
    assert_eq!(completed, format!("{user}\n{decide}"));
}

#[test]
fn test_complete_takes_a_method_naming_its_event_otherwise_as_present() {
    let fresh = file(&player(), IMPL).clone();
    let renamed = fresh
        .source
        .replace("event: MoveDecided", "decided: MoveDecided");
    assert_ne!(renamed, fresh.source);
    assert_eq!(
        complete(&fresh, &renamed, Existing::Complete)
            .unwrap()
            .completed,
        None
    );
}

/// A method the user has, its signature written otherwise than the
/// scaffold writes it: the reader takes its first input by the
/// parameter list's own brackets, so nothing inside them that is not a
/// parameter (a default's call, a comment, a quoted annotation, a
/// subscript's comma) moves it.
#[test]
fn test_complete_reads_a_signature_written_otherwise_as_present() {
    let fresh = file(&player(), IMPL).clone();
    let header = "    def decide_move(\n        self, cmd: DecideMove, state: PlayerState, ctx: CommandContext\n    )";
    assert!(fresh.source.contains(header));
    for written in [
        // A default calling a constructor: its `)` is not the list's.
        "    def decide_move(\n        self, cmd: DecideMove = DecideMove(), state: PlayerState = PlayerState(), ctx: CommandContext = None\n    )",
        // A comment holding a `)` before the input.
        "    def decide_move(\n        self,  # the logic (its own)\n        cmd: DecideMove,\n        state: PlayerState,\n        ctx: CommandContext,\n    )",
        // A quoted (forward) annotation.
        "    def decide_move(\n        self, cmd: \"DecideMove\", state: PlayerState, ctx: CommandContext\n    )",
        // The state first, a subscript with a comma in its annotation.
        "    def decide_move(\n        self, state: dict[str, int], cmd: DecideMove, ctx: CommandContext\n    )",
    ] {
        let user = fresh.source.replace(header, written);
        assert_ne!(user, fresh.source);
        assert_eq!(
            complete(&fresh, &user, Existing::Complete)
                .unwrap()
                .completed,
            None,
            "{written}"
        );
    }
}

/// A method whose input is taken as a fact where the proto declares a
/// command is refused however its signature is written.
#[test]
fn test_complete_refuses_a_command_taken_as_a_fact_behind_a_default() {
    let fresh = file(&player(), IMPL).clone();
    let user = fresh.source.replace(
        "cmd: DecideMove, state: PlayerState, ctx: CommandContext",
        "fact: DecideMove = DecideMove(), state: PlayerState = None, ctx: CommandContext = None",
    );
    assert_ne!(user, fresh.source);
    assert_eq!(
        complete(&fresh, &user, Existing::Keep),
        Err(CodegenError::Use(UseError::InputMismatch {
            path: IMPL.to_string(),
            message: "io.angzarr.examples.v1.DecideMove".to_string(),
            expected: "cmd".to_string(),
            found: "fact".to_string(),
        }))
    );
}

// ------------------------------------------------------------- field shapes

const BASKET: &[&str] = &["pyshop/v1/mood.proto", "pyshop/v1/basket.proto"];

fn basket_app() -> PythonApp {
    PythonApp {
        domain: "store".to_string(),
        protos: ProtoInput::Directory("proto".to_string()),
        rules_version: "0.1.0".to_string(),
    }
}

/// The basket's project (`tests/fixtures/pyshop`): every field shape the
/// Python scaffold maps.
fn basket() -> Vec<ScaffoldFile> {
    scaffold_python(
        &descriptors(BASKET),
        &["pyshop/v1/basket.proto"],
        &basket_app(),
    )
    .unwrap()
}

fn basket_file(path: &str) -> String {
    file(&basket(), path).source.clone()
}

const DISPATCH: &str = "src/angzarr_store/_gen/pyshop/v1/basket_state_angzarr.py";

#[test]
fn test_scaffold_python_types_each_field_shape_with_its_default() {
    let domain = basket_file("src/angzarr_store/_gen/domain.py");
    for expected in [
        "    owner: str = \"\"\n",
        "    weight: float = 0.0\n",
        "    ratio: float = 0.0\n",
        "    discount: int | None = None\n",
        "    counts: tuple[int, ...] = ()\n",
        "    last: Item | None = None\n",
        "    items: tuple[Item, ...] = ()\n",
        "    mood: Mood = Mood.UNSPECIFIED\n",
        "    feeling: Mood | None = None\n",
    ] {
        assert!(domain.contains(expected), "{expected}:\n{domain}");
    }
}

#[test]
fn test_scaffold_python_declares_an_enum_two_messages_use_once() {
    let domain = basket_file("src/angzarr_store/_gen/domain.py");
    assert_eq!(
        domain.matches("class Mood(enum.IntEnum):").count(),
        1,
        "{domain}"
    );
}

#[test]
fn test_scaffold_python_converts_a_repeated_scalar_as_a_sequence() {
    let dispatch = basket_file(DISPATCH);
    for expected in ["counts=tuple(pb.counts),", "counts=list(value.counts),"] {
        assert!(dispatch.contains(expected), "{expected}:\n{dispatch}");
    }
}

/// An `optional` scalar keeps its absence: unset on the wire is None in
/// the plain type, not the scalar's zero. Back to the wire, None goes to the
/// pb2 constructor as is, which leaves the field unset.
#[test]
fn test_scaffold_python_converts_an_optional_scalar_absent_as_none() {
    let dispatch = basket_file(DISPATCH);
    for expected in [
        "discount=pb.discount if pb.HasField(\"discount\") else None,",
        "discount=value.discount,",
    ] {
        assert!(dispatch.contains(expected), "{expected}:\n{dispatch}");
    }
}

/// An `optional` enum keeps its absence the same way: unset on the wire
/// is None, not the zero member, and None back to the wire stays None
/// rather than raising in `int()`.
#[test]
fn test_scaffold_python_converts_an_optional_enum_absent_as_none() {
    let dispatch = basket_file(DISPATCH);
    for expected in [
        "feeling=_d.Mood(pb.feeling) if pb.HasField(\"feeling\") else None,",
        "feeling=None if value.feeling is None else int(value.feeling),",
    ] {
        assert!(dispatch.contains(expected), "{expected}:\n{dispatch}");
    }
}

/// A directory input's paths are buf's `--path`s: relative to where buf
/// runs (the project root), so each is under the directory, as the
/// directory itself is.
#[test]
fn test_scaffold_python_generates_every_domain_file_its_messages_reach() {
    let buf = basket_file("buf.gen.yaml");
    assert!(
        buf.contains(
            "  - directory: proto\n    paths:\n      - proto/pyshop/v1/basket.proto\n      \
             - proto/pyshop/v1/mood.proto\nplugins:"
        ),
        "{buf}"
    );
}

#[test]
fn test_scaffold_python_keeps_a_call_of_88_columns_on_one_line() {
    let dispatch = basket_file(DISPATCH);
    let call = "    dispatch.on_command(\"pyshop.v1.EmptyTheBasketShelves\", _on_empty_the_basket_shelves)\n";
    assert_eq!(call.len(), 89);
    assert!(dispatch.contains(call), "{dispatch}");
}

#[test]
fn test_scaffold_python_wraps_the_interfaces_doc_at_79_columns() {
    let logic = basket_file("src/angzarr_store/_gen/logic.py");
    // After the licence header's line (owner ruling 2026-10-31) and the
    // generated marker's.
    let mut lines = logic.lines();
    assert!(
        lines
            .next()
            .unwrap()
            .starts_with("# Generated by angzarr-cli"),
        "{logic}"
    );
    assert!(
        angzarr_codegen::is_generated(lines.next().unwrap()),
        "{logic}"
    );
    let first = lines.next().unwrap();
    assert!(
        first.starts_with("\"\"\"StoreLogic: the store domain's logic"),
        "{logic}"
    );
    let doc: Vec<&str> = std::iter::once(first)
        .chain(lines.take_while(|l| *l != "\"\"\""))
        .collect();
    assert!(doc.len() > 2, "{logic}");
    for line in &doc {
        assert!(line.len() <= 79, "{line}");
    }
    assert!(
        doc[..doc.len() - 1].iter().all(|l| l.len() > 60),
        "{doc:#?}"
    );
}

/// A command declaring a typed refusal is scaffolded: its decision returns
/// its event or the refusal.
#[test]
fn test_scaffold_python_scaffolds_a_command_with_a_typed_refusal() {
    let app = PythonApp {
        domain: "wallet".to_string(),
        ..basket_app()
    };
    let files = scaffold_python(&descriptors(BUYIN), &["buyin/v1/wallet.proto"], &app).unwrap();
    let logic = &file(&files, "src/angzarr_wallet/_gen/logic.py").source;
    assert!(
        logic.contains(") -> list[Held] | InsufficientFunds: ..."),
        "{logic}"
    );
}

// ------------------------------------------------------- event helpers

const PLAYER_DOMAIN: &str = "src/angzarr_player/_gen/domain.py";
const MOVE_DECIDED_FROM_COMMAND: &str = r#"    @classmethod
    def from_command(
        cls,
        cmd: DecideMove,
        /,
        *,
        action: MoveAction,
    ) -> MoveDecided:
        """This event from ``cmd``: every field they share copied, the rest given.

        A missing keyword, or one naming a copied field, is a TypeError.
        """
        return cls(
            table_root=cmd.table_root,
            round=cmd.round,
            seat=cmd.seat,
            action=action,
            cards=cmd.cards,
            total=cmd.total,
            soft=cmd.soft,
            dealer_up=cmd.dealer_up,
        )
"#;

#[test]
fn test_domain_py_gives_an_event_its_commands_from_command() {
    let files = player();
    let domain = &file(&files, PLAYER_DOMAIN).source;
    let class = domain
        .split("\n\n\n")
        .find(|c| c.starts_with("@dataclass(frozen=True)\nclass MoveDecided:"))
        .expect("class MoveDecided");
    assert!(
        // The class's last newline is the split's.
        format!("{class}\n").ends_with(&format!("\n\n{MOVE_DECIDED_FROM_COMMAND}")),
        "{class}"
    );
    // A command gets none: only an emitted event does.
    let command = domain
        .split("\n\n\n")
        .find(|c| c.starts_with("@dataclass(frozen=True)\nclass DecideMove:"))
        .expect("class DecideMove");
    assert!(!command.contains("from_"), "{command}");
}

#[test]
fn test_an_event_several_commands_emit_gets_a_helper_named_for_each() {
    let domain = basket_file("src/angzarr_store/_gen/domain.py");
    assert!(
        domain.contains(
            "    @classmethod\n    def from_add_item(\n        cls,\n        cmd: AddItem,\n        /,\n    ) -> ItemAdded:\n"
        ),
        "{domain}"
    );
    assert!(
        domain.contains("        return cls(\n            item=cmd.item,\n        )\n"),
        "{domain}"
    );
    // EmptyTheBasketShelves shares no field: the item is given.
    assert!(
        domain.contains(
            "    def from_empty_the_basket_shelves(\n        cls,\n        cmd: EmptyTheBasketShelves,\n        /,\n        *,\n        item: Item | None,\n    ) -> ItemAdded:\n"
        ),
        "{domain}"
    );
    assert!(
        domain.contains("        return cls(\n            item=item,\n        )\n"),
        "{domain}"
    );
    assert!(!domain.contains("def from_command("), "{domain}");
}

/// Runs `script` with python3 beside the player's `domain.py`, an
/// `angzarr_client.router` stub standing in for the client.
fn run_player_domain(script: &str) -> std::process::Output {
    let dir = TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-from-command"));
    let client = dir.join("angzarr_client");
    std::fs::create_dir_all(&client).unwrap();
    std::fs::write(client.join("__init__.py"), "").unwrap();
    std::fs::write(
        client.join("router.py"),
        "class Bug(Exception): ...\nclass CommandContext: ...\nclass Rejection(Exception): ...\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("domain.py"),
        &file(&player(), PLAYER_DOMAIN).source,
    )
    .unwrap();
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

#[test]
fn test_from_command_copies_the_shared_fields_and_takes_the_rest() {
    let out = run_player_domain(
        "cmd = DecideMove(table_root=b't', round=2, seat=3, cards=(Card(1, Suit.CLUBS),), \
         total=11, soft=True, dealer_up=Card(5, Suit.HEARTS), can_double=True, stack=9, wager=4)\n\
         e = MoveDecided.from_command(cmd, action=MoveAction.HIT)\n\
         assert e == MoveDecided(table_root=b't', round=2, seat=3, action=MoveAction.HIT, \
         cards=(Card(1, Suit.CLUBS),), total=11, soft=True, dealer_up=Card(5, Suit.HEARTS)), e\n\
         for bad in ({}, {'action': MoveAction.HIT, 'total': 12}, {'action': MoveAction.HIT, 'nope': 1}):\n\
         \x20   try:\n\
         \x20       MoveDecided.from_command(cmd, **bad)\n\
         \x20   except TypeError:\n\
         \x20       continue\n\
         \x20   raise AssertionError(bad)\n\
         print('ok')\n",
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout), "ok\n");
}
