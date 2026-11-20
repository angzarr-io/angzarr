//! Source scanning: walks each target's module tree (following `mod`
//! declarations and `#[path]`), collects every path the code references
//! (`use` trees, expression and type paths, paths inside macro arguments,
//! `include!` files), and resolves them to module-level edges.
//!
//! Resolution follows Rust's path rules closely enough for layering:
//! `crate`/`self`/`super` prefixes, child modules, `use` aliases and
//! re-exports (`pub use proto::v1 as pb`), and crate names the package
//! depends on. A path that names nothing resolvable (a local type, a
//! prelude item, a generic) is not an edge.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

use syn::punctuated::Punctuated;
use syn::visit::{self, Visit};
use syn::{Attribute, Expr, Item, ItemUse, Token, UseTree};

use crate::model::{Edge, Model, Scope};

const ALIAS_DEPTH: usize = 16;

#[derive(Debug, Default)]
struct ModuleInfo {
    file: PathBuf,
    root: String,
    children: BTreeSet<String>,
    aliases: BTreeMap<String, Vec<String>>,
}

#[derive(Debug)]
struct RawRef {
    module: String,
    scope: Scope,
    segs: Vec<String>,
    leading_colon: bool,
    file: PathBuf,
    line: usize,
}

/// Where a module's items are being read from.
#[derive(Clone)]
struct Ctx {
    module: String,
    file: PathBuf,
    /// Directory `mod x;` declarations of this module resolve against.
    child_dir: PathBuf,
    scope: Scope,
}

pub struct Scanner {
    workspace_root: PathBuf,
    memory: Option<BTreeMap<PathBuf, String>>,
    modules: BTreeMap<String, ModuleInfo>,
    known: BTreeMap<String, BTreeSet<String>>,
    workspace_crates: BTreeSet<String>,
    refs: Vec<RawRef>,
    edges: Vec<Edge>,
    warnings: Vec<String>,
}

impl Scanner {
    pub fn new(workspace_root: PathBuf) -> Self {
        Scanner {
            workspace_root,
            memory: None,
            modules: BTreeMap::new(),
            known: BTreeMap::new(),
            workspace_crates: BTreeSet::new(),
            refs: Vec::new(),
            edges: Vec::new(),
            warnings: Vec::new(),
        }
    }

    /// A scanner that reads sources from `files` instead of the disk.
    #[cfg(test)]
    pub fn in_memory(workspace_root: PathBuf, files: BTreeMap<PathBuf, String>) -> Self {
        Scanner {
            memory: Some(files),
            ..Scanner::new(workspace_root)
        }
    }

    pub fn add_workspace_crate(&mut self, lib: &str) {
        self.workspace_crates.insert(lib.to_string());
    }

    /// Scans one target rooted at `src` as module `root`. `known` holds the
    /// crate names its code may reference (its dependencies, its own library
    /// and the standard crates).
    pub fn add_target(
        &mut self,
        root: &str,
        src: &Path,
        scope: Scope,
        known: &BTreeSet<String>,
    ) -> Result<(), String> {
        self.known.insert(root.to_string(), known.clone());
        let ctx = Ctx {
            module: root.to_string(),
            file: src.to_path_buf(),
            child_dir: parent_dir(src),
            scope,
        };
        self.walk_file(root, ctx)
    }

    pub fn add_manifest_edge(
        &mut self,
        from: &str,
        to: &str,
        scope: Scope,
        manifest: &Path,
        via: &str,
    ) {
        self.edges.push(Edge {
            from: from.to_string(),
            to: to.to_string(),
            scope,
            file: self.relative(manifest),
            line: 0,
            via: via.to_string(),
        });
    }

    /// `path` relative to the workspace root, lexically normalised.
    pub fn relative(&self, path: &Path) -> PathBuf {
        let path = normalise(path);
        path.strip_prefix(&self.workspace_root)
            .map(Path::to_path_buf)
            .unwrap_or(path)
    }

    pub fn finish(mut self) -> (Model, Vec<String>) {
        let refs = std::mem::take(&mut self.refs);
        let mut edges = std::mem::take(&mut self.edges);
        for r in &refs {
            let home = self
                .modules
                .get(&r.module)
                .map(|m| m.root.clone())
                .unwrap_or_default();
            if let Some(to) = self.resolve(&r.module, &r.segs, r.leading_colon, &home, 0) {
                if to != r.module {
                    edges.push(Edge {
                        from: r.module.clone(),
                        to,
                        scope: r.scope,
                        file: self.relative(&r.file),
                        line: r.line,
                        via: r.segs.join("::"),
                    });
                }
            }
        }
        edges.sort();
        edges.dedup();
        let model = Model {
            modules: self
                .modules
                .iter()
                .map(|(name, info)| (name.clone(), self.relative(&info.file)))
                .collect(),
            workspace_crates: self.workspace_crates.clone(),
            edges,
        };
        (model, self.warnings)
    }

    fn read(&self, path: &Path) -> Option<String> {
        match &self.memory {
            Some(files) => files.get(path).cloned(),
            None => std::fs::read_to_string(path).ok(),
        }
    }

    fn exists(&self, path: &Path) -> bool {
        match &self.memory {
            Some(files) => files.contains_key(path),
            None => path.is_file(),
        }
    }

    fn register(&mut self, root: &str, ctx: &Ctx) {
        let info = self.modules.entry(ctx.module.clone()).or_default();
        info.file = ctx.file.clone();
        info.root = root.to_string();
        if let Some((parent, name)) = ctx.module.rsplit_once("::") {
            if let Some(p) = self.modules.get_mut(parent) {
                p.children.insert(name.to_string());
            }
        }
    }

    fn walk_file(&mut self, root: &str, ctx: Ctx) -> Result<(), String> {
        let text = self
            .read(&ctx.file)
            .ok_or_else(|| format!("{}: cannot read", ctx.file.display()))?;
        let ast = syn::parse_file(&text).map_err(|e| format!("{}: {e}", ctx.file.display()))?;
        self.register(root, &ctx);
        self.walk_items(root, &ctx, &ast.items)
    }

    fn walk_items(&mut self, root: &str, ctx: &Ctx, items: &[Item]) -> Result<(), String> {
        for item in items {
            let scope = if is_cfg_test(item_attrs(item)) {
                Scope::Test
            } else {
                ctx.scope
            };
            match item {
                Item::Mod(m) => self.walk_mod(root, ctx, m, scope)?,
                Item::Use(u) => self.record_use(ctx, u, scope, true),
                other => {
                    let mut c = Collector::new(ctx, scope);
                    c.visit_item(other);
                    let (refs, includes) = (c.refs, c.includes);
                    self.refs.extend(refs);
                    for (line, lit) in includes {
                        self.record_include(ctx, scope, line, &lit);
                    }
                }
            }
        }
        Ok(())
    }

    fn walk_mod(
        &mut self,
        root: &str,
        ctx: &Ctx,
        m: &syn::ItemMod,
        scope: Scope,
    ) -> Result<(), String> {
        let name = m.ident.to_string();
        let child = Ctx {
            module: format!("{}::{name}", ctx.module),
            file: ctx.file.clone(),
            child_dir: ctx.child_dir.join(&name),
            scope,
        };
        if let Some((_, items)) = &m.content {
            self.register(root, &child);
            return self.walk_items(root, &child, items);
        }
        let (file, child_dir) = match path_attr(&m.attrs) {
            Some(p) => {
                let file = parent_dir(&ctx.file).join(p);
                let dir = parent_dir(&file);
                (file, dir)
            }
            None => {
                let flat = ctx.child_dir.join(format!("{name}.rs"));
                let nested = ctx.child_dir.join(&name).join("mod.rs");
                if self.exists(&flat) {
                    (flat, ctx.child_dir.join(&name))
                } else if self.exists(&nested) {
                    (nested, ctx.child_dir.join(&name))
                } else {
                    self.warnings.push(format!(
                        "{}: module `{name}` has no source file",
                        self.relative(&ctx.file).display()
                    ));
                    return Ok(());
                }
            }
        };
        self.walk_file(
            root,
            Ctx {
                file: normalise(&file),
                child_dir,
                ..child
            },
        )
    }

    /// Records a `use` item: each leaf path is a reference; at module level
    /// each imported name also becomes an alias resolvable by later paths.
    fn record_use(&mut self, ctx: &Ctx, u: &ItemUse, scope: Scope, module_level: bool) {
        let line = u.use_token.span.start().line;
        let mut leaves = Vec::new();
        flatten_use(&u.tree, Vec::new(), &mut leaves);
        for (segs, alias) in leaves {
            if module_level {
                if let (Some(alias), Some(info)) = (alias, self.modules.get_mut(&ctx.module)) {
                    info.aliases.insert(alias, segs.clone());
                }
            }
            self.refs.push(RawRef {
                module: ctx.module.clone(),
                scope,
                segs,
                leading_colon: u.leading_colon.is_some(),
                file: ctx.file.clone(),
                line,
            });
        }
    }

    fn record_include(&mut self, ctx: &Ctx, scope: Scope, line: usize, lit: &str) {
        let target = self.relative(&parent_dir(&ctx.file).join(lit));
        self.edges.push(Edge {
            from: ctx.module.clone(),
            to: format!("path:{}", target.display()),
            scope,
            file: self.relative(&ctx.file),
            line,
            via: format!("include!(\"{lit}\")"),
        });
    }

    /// Resolves a path written in `module` to the deepest workspace module it
    /// names, or to the external path as written. `None` when the path names
    /// nothing resolvable from `module`.
    ///
    /// `home` is the root of the referencing crate: `use` aliases are followed
    /// only inside it. A path into another crate ends at the module that
    /// re-exports the item — that crate's public surface — not at the private
    /// module defining it.
    fn resolve(
        &self,
        module: &str,
        segs: &[String],
        leading_colon: bool,
        home: &str,
        depth: usize,
    ) -> Option<String> {
        let info = self.modules.get(module)?;
        let (first, rest) = segs.split_first()?;
        let known = self.known.get(&info.root);
        let is_crate = |name: &str| {
            known.is_some_and(|k| k.contains(name)) || self.workspace_crates.contains(name)
        };
        let start = match first.as_str() {
            _ if leading_colon => return self.enter_crate(first, rest, home, depth),
            "crate" => info.root.clone(),
            "self" => module.to_string(),
            "super" => parent_of(module, &info.root),
            name if info.children.contains(name) => format!("{module}::{name}"),
            name if info.aliases.contains_key(name) && depth < ALIAS_DEPTH => {
                let target = self.resolve(module, &info.aliases[name], false, home, depth + 1)?;
                return self.continue_from(target, rest, home, depth);
            }
            name if is_crate(name) => return self.enter_crate(first, rest, home, depth),
            _ => return None,
        };
        Some(self.walk(start, rest, home, depth))
    }

    fn enter_crate(&self, name: &str, rest: &[String], home: &str, depth: usize) -> Option<String> {
        if self.workspace_crates.contains(name) && self.modules.contains_key(name) {
            return Some(self.walk(name.to_string(), rest, home, depth));
        }
        Some(join(name, rest))
    }

    fn continue_from(
        &self,
        target: String,
        rest: &[String],
        home: &str,
        depth: usize,
    ) -> Option<String> {
        if self.modules.contains_key(&target) {
            Some(self.walk(target, rest, home, depth))
        } else {
            Some(join(&target, rest))
        }
    }

    /// Descends from module `cur` through `rest` while each segment names a
    /// child module (or, inside `home`, an alias of one); stops at the first
    /// item.
    fn walk(&self, mut cur: String, rest: &[String], home: &str, depth: usize) -> String {
        for (i, seg) in rest.iter().enumerate() {
            let Some(info) = self.modules.get(&cur) else {
                break;
            };
            if seg == "super" {
                cur = parent_of(&cur, &info.root);
            } else if seg == "self" {
                continue;
            } else if info.children.contains(seg) {
                cur = format!("{cur}::{seg}");
            } else if let Some(alias) = info
                .aliases
                .get(seg)
                .filter(|_| depth < ALIAS_DEPTH && info.root == home)
            {
                return match self.resolve(&cur, alias, false, home, depth + 1) {
                    Some(target) => self
                        .continue_from(target, &rest[i + 1..], home, depth + 1)
                        .unwrap_or(cur),
                    None => cur,
                };
            } else {
                break;
            }
        }
        cur
    }
}

/// Collects every path referenced inside one item.
struct Collector {
    ctx: Ctx,
    scopes: Vec<Scope>,
    refs: Vec<RawRef>,
    includes: Vec<(usize, String)>,
}

impl Collector {
    fn new(ctx: &Ctx, scope: Scope) -> Self {
        Collector {
            ctx: ctx.clone(),
            scopes: vec![scope],
            refs: Vec::new(),
            includes: Vec::new(),
        }
    }

    fn scope(&self) -> Scope {
        *self.scopes.last().unwrap_or(&Scope::Lib)
    }

    fn with_attrs(&mut self, attrs: &[Attribute], f: impl FnOnce(&mut Self)) {
        let scope = if is_cfg_test(attrs) {
            Scope::Test
        } else {
            self.scope()
        };
        self.scopes.push(scope);
        f(self);
        self.scopes.pop();
    }

    fn push(&mut self, segs: Vec<String>, leading_colon: bool, line: usize) {
        self.refs.push(RawRef {
            module: self.ctx.module.clone(),
            scope: self.scope(),
            segs,
            leading_colon,
            file: self.ctx.file.clone(),
            line,
        });
    }
}

impl<'ast> Visit<'ast> for Collector {
    fn visit_path(&mut self, p: &'ast syn::Path) {
        // A bare identifier is a local, a generic or an imported name (whose
        // `use` is already an edge) — never a module reference on its own.
        if p.segments.len() > 1 || p.leading_colon.is_some() {
            let segs = p.segments.iter().map(|s| s.ident.to_string()).collect();
            let line = p
                .segments
                .first()
                .map_or(0, |s| s.ident.span().start().line);
            self.push(segs, p.leading_colon.is_some(), line);
        }
        visit::visit_path(self, p);
    }

    fn visit_item_use(&mut self, u: &'ast ItemUse) {
        let line = u.use_token.span.start().line;
        let mut leaves = Vec::new();
        flatten_use(&u.tree, Vec::new(), &mut leaves);
        for (segs, _) in leaves {
            self.push(segs, u.leading_colon.is_some(), line);
        }
    }

    fn visit_item_mod(&mut self, _: &'ast syn::ItemMod) {}

    fn visit_item(&mut self, i: &'ast Item) {
        self.with_attrs(item_attrs(i), |c| visit::visit_item(c, i));
    }

    fn visit_impl_item(&mut self, i: &'ast syn::ImplItem) {
        let attrs: &[Attribute] = match i {
            syn::ImplItem::Fn(f) => &f.attrs,
            syn::ImplItem::Const(c) => &c.attrs,
            syn::ImplItem::Type(t) => &t.attrs,
            _ => &[],
        };
        self.with_attrs(attrs, |c| visit::visit_impl_item(c, i));
    }

    fn visit_macro(&mut self, m: &'ast syn::Macro) {
        let line = m
            .path
            .segments
            .first()
            .map_or(0, |s| s.ident.span().start().line);
        let name = m
            .path
            .segments
            .last()
            .map(|s| s.ident.to_string())
            .unwrap_or_default();
        if matches!(name.as_str(), "include" | "include_str" | "include_bytes") {
            if let Ok(lit) = m.parse_body::<syn::LitStr>() {
                self.includes.push((line, lit.value()));
            }
        }
        if let Ok(args) = m.parse_body_with(Punctuated::<Expr, Token![,]>::parse_terminated) {
            for arg in &args {
                self.visit_expr(arg);
            }
        }
        visit::visit_macro(self, m);
    }
}

/// Flattens a use tree into (path, imported name) leaves. A glob yields its
/// prefix with no name; `self` names its parent; `as _` imports no name.
fn flatten_use(tree: &UseTree, prefix: Vec<String>, out: &mut Vec<(Vec<String>, Option<String>)>) {
    let extend = |seg: String| {
        let mut p = prefix.clone();
        p.push(seg);
        p
    };
    match tree {
        UseTree::Path(p) => flatten_use(&p.tree, extend(p.ident.to_string()), out),
        UseTree::Name(n) if n.ident == "self" => {
            let name = prefix.last().cloned();
            out.push((prefix, name));
        }
        UseTree::Name(n) => out.push((extend(n.ident.to_string()), Some(n.ident.to_string()))),
        UseTree::Rename(r) => {
            let path = if r.ident == "self" {
                prefix
            } else {
                extend(r.ident.to_string())
            };
            let name = (r.rename != "_").then(|| r.rename.to_string());
            out.push((path, name));
        }
        UseTree::Glob(_) => out.push((prefix, None)),
        UseTree::Group(g) => {
            for t in &g.items {
                flatten_use(t, prefix.clone(), out);
            }
        }
    }
}

fn item_attrs(item: &Item) -> &[Attribute] {
    match item {
        Item::Const(i) => &i.attrs,
        Item::Enum(i) => &i.attrs,
        Item::Fn(i) => &i.attrs,
        Item::ForeignMod(i) => &i.attrs,
        Item::Impl(i) => &i.attrs,
        Item::Macro(i) => &i.attrs,
        Item::Mod(i) => &i.attrs,
        Item::Static(i) => &i.attrs,
        Item::Struct(i) => &i.attrs,
        Item::Trait(i) => &i.attrs,
        Item::Type(i) => &i.attrs,
        Item::Union(i) => &i.attrs,
        Item::Use(i) => &i.attrs,
        _ => &[],
    }
}

/// True iff the attributes gate the item on `test` (and not on `not(test)`).
pub fn is_cfg_test(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|a| {
        if !a.path().is_ident("cfg") {
            return false;
        }
        let Ok(list) = a.meta.require_list() else {
            return false;
        };
        let text = list.tokens.to_string();
        let words: Vec<&str> = text
            .split(|c: char| !(c.is_alphanumeric() || c == '_'))
            .filter(|w| !w.is_empty())
            .collect();
        words.contains(&"test") && !words.contains(&"not")
    })
}

fn path_attr(attrs: &[Attribute]) -> Option<String> {
    attrs
        .iter()
        .find(|a| a.path().is_ident("path"))
        .and_then(|a| {
            let nv = a.meta.require_name_value().ok()?;
            match &nv.value {
                Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(s),
                    ..
                }) => Some(s.value()),
                _ => None,
            }
        })
}

fn parent_of(module: &str, root: &str) -> String {
    if module == root {
        return root.to_string();
    }
    module
        .rsplit_once("::")
        .map_or(root.to_string(), |(p, _)| p.to_string())
}

fn parent_dir(path: &Path) -> PathBuf {
    path.parent().map(Path::to_path_buf).unwrap_or_default()
}

fn join(head: &str, rest: &[String]) -> String {
    std::iter::once(head.to_string())
        .chain(rest.iter().cloned())
        .collect::<Vec<_>>()
        .join("::")
}

/// Lexical normalisation: drops `.` and folds `..` without touching the disk.
fn normalise(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
    }
    out
}

#[cfg(test)]
#[path = "scan.test.rs"]
mod scan_tests;
