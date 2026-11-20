# Messages for this crate's diagnostics. Each id is a diagnostic's code
# without the crate's name, which the build prefixes
# (angzarr_diagnostic::build); the variables are its args.

no-out-dir = OUT_DIR is unset: run this from a build script
io = { $path }: { $detail }
protoc = protoc failed: { $detail }
prost = prost-build failed: { $detail }
codegen = { $error }
buf = buf generate failed: { $detail }
uv = uv failed: { $detail }
generated-unmarked = { $path } is where a generated file goes, but it does not carry the generated marker (@generated): it was edited by hand, or written in its place, and is not overwritten
generated-unmarked-remedy = move your changes out of it (generated files are rewritten on every run; your code goes in the files written once), then restore it or delete it so the next run writes it. A file an older angzarr-cli wrote without the marker is safe to delete
