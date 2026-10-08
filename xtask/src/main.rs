//! Build automation tasks for angzarr.
//!
//! Run with: `cargo xtask <command>`
//!
//! Available commands:
//! - `gen-mutants-exclude`: Scan for `#[trivial_delegation]` and update mutants.toml

use std::{collections::BTreeSet, fs, path::Path};
use syn::{visit::Visit, Attribute, ImplItem, ItemFn, ItemImpl, ItemTrait, TraitItem};

fn main() {
    let args: Vec<_> = std::env::args().collect();

    match args.get(1).map(|s| s.as_str()) {
        Some("gen-mutants-exclude") => gen_mutants_exclude(),
        Some("help") | Some("--help") | Some("-h") => print_help(),
        Some(cmd) => {
            eprintln!("Unknown command: {cmd}");
            print_help();
            std::process::exit(1);
        }
        None => {
            print_help();
            std::process::exit(1);
        }
    }
}

fn print_help() {
    eprintln!(
        r#"Usage: cargo xtask <command>

Commands:
    gen-mutants-exclude    Scan for #[trivial_delegation] attributes and
                           generate exclude_re patterns in .cargo/mutants.toml
    help                   Show this help message
"#
    );
}

fn gen_mutants_exclude() {
    let mut excludes = BTreeSet::new();

    // Find all Rust source files
    let patterns = ["src/**/*.rs", "crates/**/*.rs"];

    for pattern in patterns {
        for entry in glob::glob(pattern).expect("Invalid glob pattern").flatten() {
            if let Some(found) = scan_file(&entry) {
                excludes.extend(found);
            }
        }
    }

    if excludes.is_empty() {
        println!("No #[trivial_delegation] attributes found.");
        return;
    }

    println!(
        "Found {} functions marked with #[trivial_delegation]:",
        excludes.len()
    );
    for ex in &excludes {
        println!("  - {ex}");
    }

    // Update mutants.toml
    update_mutants_toml(&excludes);
}

fn scan_file(path: &Path) -> Option<Vec<String>> {
    let content = fs::read_to_string(path).ok()?;
    let file = syn::parse_file(&content).ok()?;

    let mut visitor = TrivialDelegationVisitor::new();
    visitor.visit_file(&file);

    if visitor.found.is_empty() {
        None
    } else {
        Some(visitor.found)
    }
}

struct TrivialDelegationVisitor {
    current_impl: Option<ImplContext>,
    found: Vec<String>,
}

struct ImplContext {
    self_type: String,
    trait_name: Option<String>,
}

impl TrivialDelegationVisitor {
    fn new() -> Self {
        Self {
            current_impl: None,
            found: Vec::new(),
        }
    }

    fn add_function(&mut self, fn_name: &str) {
        // cargo-mutants --list output format:
        // - Methods: "TypeName::method_name"
        // - Trait impls: "<impl TraitName for TypeName>::method_name"
        let regex = if let Some(ctx) = &self.current_impl {
            if let Some(trait_name) = &ctx.trait_name {
                // impl Trait for Type -> "<impl Trait for Type>::method"
                // Use .* before trait name to match module paths like "client_traits::GatewayClient"
                format!(
                    r"<impl .*{}.*for {}>::{}",
                    regex_escape(trait_name),
                    regex_escape(&ctx.self_type),
                    regex_escape(fn_name)
                )
            } else {
                // impl Type -> "Type::method"
                format!(
                    r"{}::{}",
                    regex_escape(&ctx.self_type),
                    regex_escape(fn_name)
                )
            }
        } else {
            // Free function -> just the function name
            format!(r"::{}", regex_escape(fn_name))
        };

        self.found.push(regex);
    }
}

impl<'ast> Visit<'ast> for TrivialDelegationVisitor {
    fn visit_item_impl(&mut self, node: &'ast ItemImpl) {
        let self_type = type_to_string(&node.self_ty);
        let trait_name = node
            .trait_
            .as_ref()
            .map(|(_, path, _)| path_to_string(path));

        self.current_impl = Some(ImplContext {
            self_type,
            trait_name,
        });

        syn::visit::visit_item_impl(self, node);

        self.current_impl = None;
    }

    fn visit_item_trait(&mut self, node: &'ast ItemTrait) {
        // Default method bodies on a trait: cargo-mutants names them
        // "TraitName::method".
        for item in &node.items {
            if let TraitItem::Fn(method) = item {
                if method.default.is_some() && has_trivial_delegation(&method.attrs) {
                    self.found.push(format!(
                        r"{}::{}",
                        regex_escape(&node.ident.to_string()),
                        regex_escape(&method.sig.ident.to_string())
                    ));
                }
            }
        }
        syn::visit::visit_item_trait(self, node);
    }

    fn visit_impl_item(&mut self, node: &'ast ImplItem) {
        if let ImplItem::Fn(method) = node {
            if has_trivial_delegation(&method.attrs) {
                self.add_function(&method.sig.ident.to_string());
            }
        }
        syn::visit::visit_impl_item(self, node);
    }

    fn visit_item_fn(&mut self, node: &'ast ItemFn) {
        if has_trivial_delegation(&node.attrs) {
            self.add_function(&node.sig.ident.to_string());
        }
        syn::visit::visit_item_fn(self, node);
    }
}

fn has_trivial_delegation(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        let path = attr.path();
        // Match both `trivial_delegation` and `crate::trivial_delegation`
        path.segments
            .last()
            .map(|s| s.ident == "trivial_delegation")
            .unwrap_or(false)
    })
}

fn type_to_string(ty: &syn::Type) -> String {
    match ty {
        syn::Type::Path(p) => path_to_string(&p.path),
        _ => ".*".to_string(),
    }
}

fn path_to_string(path: &syn::Path) -> String {
    path.segments
        .last()
        .map(|s| s.ident.to_string())
        .unwrap_or_else(|| ".*".to_string())
}

fn regex_escape(s: &str) -> String {
    // Escape regex special characters
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '.' | '+' | '*' | '?' | '^' | '$' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '\\' => {
                result.push('\\');
                result.push(c);
            }
            _ => result.push(c),
        }
    }
    result
}

fn update_mutants_toml(excludes: &BTreeSet<String>) {
    let path = Path::new(".cargo/mutants.toml");

    let content = fs::read_to_string(path).expect("Failed to read .cargo/mutants.toml");
    let mut doc = content
        .parse::<toml_edit::DocumentMut>()
        .expect("Failed to parse .cargo/mutants.toml");

    // Build new array with multi-line formatting
    // All exclude_re entries are auto-generated from #[trivial_delegation]
    let mut new_arr = toml_edit::Array::new();
    new_arr.set_trailing_comma(true);
    new_arr.set_trailing("\n");

    // Add comment as first entry decoration
    let mut first = true;
    for ex in excludes {
        let mut val = toml_edit::Value::from(ex.as_str());
        if first {
            val.decor_mut()
                .set_prefix("\n  # Auto-generated by: cargo xtask gen-mutants-exclude\n  ");
            first = false;
        } else {
            val.decor_mut().set_prefix("\n  ");
        }
        new_arr.push_formatted(val);
    }

    // Set the array in the document
    doc["exclude_re"] = toml_edit::value(new_arr);

    // Write back
    fs::write(path, doc.to_string()).expect("Failed to write .cargo/mutants.toml");

    println!(
        "\nUpdated .cargo/mutants.toml with {} exclude patterns.",
        excludes.len()
    );
}
