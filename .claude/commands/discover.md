---
description: Discover and install profiles, bundles, and fragments
---


Scan the current project and discover matching ctxloom content from configured remotes.

## Steps

1. **Scan the project directory** for indicators like:
   - go.mod, Cargo.toml, package.json, pyproject.toml, requirements.txt
   - Dockerfile, docker-compose.yml, Makefile, justfile
   - .github/, .gitlab-ci.yml, and other CI/CD configs
   - Framework-specific files (next.config.js, vite.config.ts, etc.)

2. **Search across all configured remotes** using ctxloom MCP tools:
   - Use `list_remotes` to see all configured remotes
   - Use `search_remotes` to find matching content:
     - Search by tags: "tag:golang", "tag:react", "tag:docker"
     - Search by text: "security", "testing", "ci-cd"
   - Use `browse_remote` to explore specific remotes in detail

3. **Present your findings**:
   - What project type/stack you detected
   - Matching content from each remote:
     - **Profiles**: Development workflow configurations
     - **Bundles**: Collections of fragments (context) and prompts (reusable commands)
   - Ask the user which items to install

4. **Install selected items** using:
   - `pull_remote` to fetch bundles/profiles from remotes
   - `sync_dependencies` to ensure all dependencies are fetched

## Example workflow

1. Detect go.mod -> search for "tag:golang" across all remotes
2. Detect Dockerfile -> search for "tag:docker" and "tag:container"
3. Present all matches grouped by remote, let user choose
4. Pull selected items and sync dependencies

If the user says "skip", acknowledge and let them know they can run `/discover` again later.
