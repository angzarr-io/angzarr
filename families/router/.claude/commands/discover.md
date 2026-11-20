---
description: Discover and install profiles, bundles, and fragments
---


Scan the current project and discover matching ctxloom content from configured remotes.

## Surface (read this first)

- **Discovery is the `search_library` MCP tool.** It searches every configured
  remote by reading their local git clones (no network) and returns matching
  bundles and profiles, each with a `pull_ref` you reference from a local profile.
  - Search by tag: `tag:golang`, `tag:react`, `tag:docker`.
  - Search by text: `security`, `testing`, `ci-cd`.
  - Optionally pass `item_type` (`bundle` or `profile`) to narrow.
- **`search_content` searches LOCAL content only** (bundles/profiles already
  installed in this project's cache). It does NOT reach remotes — do not use it
  to discover remote content.
- **Listings are MCP resources.** Read `ctxloom://remotes` for the configured
  remotes, and `ctxloom://profiles` / `ctxloom://fragments` / `ctxloom://prompts`
  for what is already installed locally.
- **Consumption is CLI and reference-only.** You author a local profile that
  references remote content (`ctxloom profile create <name> --parent <ref>` for a
  remote profile, or `-b <ref>` for a bundle), then `ctxloom remote pull` fetches
  the referenced bundles/profiles and updates the lockfile.

## Steps

1. **Scan the project directory** for indicators like:
   - go.mod, Cargo.toml, package.json, pyproject.toml, requirements.txt
   - Dockerfile, docker-compose.yml, Makefile, justfile
   - .github/, .gitlab-ci.yml, and other CI/CD configs
   - Framework-specific files (next.config.js, vite.config.ts, etc.)

2. **(Optional) List configured remotes** by reading `ctxloom://remotes`.

3. **Search the remotes** with the `search_library` MCP tool, using tags/text
   derived from the stack you detected (e.g. `tag:golang`, `tag:docker`,
   `python-development`, `web-frontend`). Each result's `pull_ref` (e.g.
   `ctxloom-default/go-developer`) is the remote ref you reference from a local
   profile.

4. **Present your findings**:
   - What project type/stack you detected
   - Matching content grouped by remote:
     - **Profiles**: Development workflow configurations
     - **Bundles**: Collections of fragments (context) and prompts (reusable commands)
   - Ask the user which items to reference

5. **Reference selected items** from a local profile, then pull:
   - Inherit a remote profile:
     `ctxloom profile create <name> --parent <pull_ref>`
     (e.g. `ctxloom profile create go-dev --parent ctxloom-default/go-developer`)
   - Reference a remote bundle (or one fragment):
     `ctxloom profile create <name> -b <pull_ref>` (optionally `#fragments/<frag>`)
   - Run `ctxloom remote pull` afterward so every bundle/profile a profile
     references is fetched into the cache and the lockfile is updated.
   - To pin a specific content version, append a git tag or commit SHA to the
     ref with `@`: `ctxloom-default/go-developer@v1.2.0`. Unpinned refs track
     the remote's default branch.
   - Make a profile the default with `ctxloom profile default <name>` so
     `ctxloom run` loads it automatically. Defaults are a list; a default may be a
     local name or a remote ref, and `--unset <name>` clears one.

## Example workflow

1. Read `ctxloom://remotes` -> `ctxloom-default` (and any personal remotes) are configured
2. Detect go.mod + Dockerfile -> `search_library` with `tag:golang`, then `tag:docker`
3. Spot the `go-developer` profile and `go-ai-practices`/`container` bundles in the results
4. Present matches grouped by remote, let the user choose
5. `ctxloom profile create go-dev --parent ctxloom-default/go-developer`, then run `ctxloom remote pull`

If the user says "skip", acknowledge and let them know they can run `/discover` again later.
