//! archlint — module-level architecture rules for a Cargo workspace.
//!
//! Builds a module dependency model of every workspace target (source `use`
//! and path references, `include!` files, manifest dependencies) and checks
//! it against the rules in `archlint.toml` (see [`rules`] for the format).
//!
//! ```text
//! archlint [--rules archlint.toml] [--manifest-path Cargo.toml] [--edges]
//! ```
//!
//! `--edges` prints the model (one `scope from -> to` line per edge) instead
//! of checking it: the starting point when writing rules for a new repo.
//! Exit status: 0 clean, 1 violations, 2 a configuration or scan error.

mod model;
mod rules;
mod scan;
mod workspace;

use std::path::PathBuf;
use std::process::ExitCode;

use model::{Model, Scope};

struct Args {
    rules: PathBuf,
    manifest: PathBuf,
    edges: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        rules: PathBuf::from("archlint.toml"),
        manifest: PathBuf::from("Cargo.toml"),
        edges: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--rules" => args.rules = it.next().ok_or("--rules needs a path")?.into(),
            "--manifest-path" => {
                args.manifest = it.next().ok_or("--manifest-path needs a path")?.into()
            }
            "--edges" => args.edges = true,
            other => return Err(format!("unknown argument `{other}`")),
        }
    }
    Ok(args)
}

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(e) => {
            eprintln!("archlint: {e}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<bool, String> {
    let args = parse_args()?;
    let meta = workspace::metadata(&args.manifest)?;
    let (model, warnings) = workspace::build_model(&meta)?;
    for w in &warnings {
        eprintln!("archlint: warning: {w}");
    }
    if args.edges {
        print_edges(&model);
        return Ok(true);
    }
    let file = rules::load(&args.rules)?;
    let stale = rules::unmatched_from(&file, &model);
    if !stale.is_empty() {
        let list: Vec<String> = stale
            .iter()
            .map(|(rule, pattern)| {
                format!("rule `{rule}`: `from` pattern `{pattern}` matches no module")
            })
            .collect();
        return Err(list.join("\n"));
    }
    let violations = rules::check(&file, &model);
    if violations.is_empty() {
        println!(
            "archlint: {} rules, {} modules, {} edges: no violations",
            file.rules.len(),
            model.modules.len(),
            model.edges.len()
        );
        return Ok(true);
    }
    eprintln!(
        "archlint: {} violation(s) of {}",
        violations.len(),
        args.rules.display()
    );
    for v in &violations {
        let at = match v.edge.line {
            0 => v.edge.file.display().to_string(),
            line => format!("{}:{line}", v.edge.file.display()),
        };
        eprintln!(
            "\n[{}] {}\n  {at}  {} -> {}  (via {})",
            v.rule.name, v.rule.why, v.edge.from, v.edge.to, v.edge.via
        );
    }
    Ok(false)
}

fn print_edges(model: &Model) {
    let lines: std::collections::BTreeSet<String> = model
        .edges
        .iter()
        .map(|e| {
            let scope = match e.scope {
                Scope::Lib => "lib ",
                Scope::Test => "test",
            };
            format!("{scope} {} -> {}", e.from, e.to)
        })
        .collect();
    for line in lines {
        println!("{line}");
    }
}
