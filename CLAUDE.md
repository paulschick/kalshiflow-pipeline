## Communication Mode

- **Always use `caveman` skill in `ultra` intensity** for all chat responses. Load at session start;
  persist for entire session. Code, commits, PRs, and file content remain normal prose. Drop caveman
  for security warnings, destructive-op confirmations, and any case where compression risks misread —
  resume after the unambiguous part.

## Commit Rules

- No attribution, Co-Authored-By, or extra commentary unless explicitly requested.
- Messages are imperative and focused target change(s).

<!-- code-review-graph MCP tools -->

## MCP Tools: code-review-graph

**IMPORTANT: This project has a knowledge graph. ALWAYS use the
code-review-graph MCP tools BEFORE using Grep/Glob/Read to explore
the codebase.** The graph is faster, cheaper (fewer tokens), and gives
you structural context (callers, dependents, test coverage) that file
scanning cannot.

### When to use graph tools FIRST

- **Exploring code**: `semantic_search_nodes` or `query_graph` instead of Grep
- **Understanding impact**: `get_impact_radius` instead of manually tracing imports
- **Code review**: `detect_changes` + `get_review_context` instead of reading entire files
- **Finding relationships**: `query_graph` with callers_of/callees_of/imports_of/tests_for
- **Architecture questions**: `get_architecture_overview` + `list_communities`

Fall back to Grep/Glob/Read **only** when the graph doesn't cover what you need.

### Key Tools

| Tool                        | Use when                                               |
|-----------------------------|--------------------------------------------------------|
| `detect_changes`            | Reviewing code changes — gives risk-scored analysis    |
| `get_review_context`        | Need source snippets for review — token-efficient      |
| `get_impact_radius`         | Understanding blast radius of a change                 |
| `get_affected_flows`        | Finding which execution paths are impacted             |
| `query_graph`               | Tracing callers, callees, imports, tests, dependencies |
| `semantic_search_nodes`     | Finding functions/classes by name or keyword           |
| `get_architecture_overview` | Understanding high-level codebase structure            |
| `refactor_tool`             | Planning renames, finding dead code                    |

### Workflow

1. The graph auto-updates on file changes (via hooks).
2. Use `detect_changes` for code review.
3. Use `get_affected_flows` to understand impact.
4. Use `query_graph` pattern="tests_for" to check coverage.

## MCP Tools: gcp-cost (cost claims)

**Never make a $/mo or other monetary claim without verifying via the
`gcp-cost` MCP server against real data.** Round-number guesses, scaled-from-prior estimates, and back-of-envelope
extrapolations are all forbidden in committed docs. Cost claims are facts; like any fact, they must cite a source.

### Required workflow before stating any cost

1. **Get inputs from real measurements**, not guesses. Volumes via `task ops:bq:cost:window SINCE=24h`; live resource
   shape via `task ops:worker:status` / `gcloud run … describe`; metric volume via Cloud Monitoring REST.
2. **For each service that contributes**: `mcp__gcp-cost__list_services` → `list_skus` (filter by region + keyword)
   → `estimate_cost` with the SKU id and the measured usage. Cite the SKU id in the doc.
3. **Don't omit services** because they "feel small" — write `~$0` with a free-tier source rather than skipping. A
   reader should see the full breakdown and trust nothing was hand-waved.
4. **Watch SKU units.** Pub/Sub Message Delivery is priced per TiB; passing GiB as the usage amount produces a 1024×
   over-estimate. Always sanity-check the `unit:` field in the estimate response.
5. **Free-tier scope.** Cloud Run free tier is per billing account per month; one always-on workload eats it before
   downstream jobs see any. Don't double-count it.

When MCP auth fails (`invalid_rapt`), surface the exact `gcloud auth application-default login` command to the user;
do NOT fall back to extrapolating the number. Stale costs are dishonest.

## Task runner

`task <name>` invocations from a non-TTY caller (Claude Code Bash tool, subagent shells, CI) must pass `-y` —
several `task ops:*` tasks (notably `ops:image:push` and any task that includes it, e.g. `ops:image:deploy`) use
go-task's `prompt:` field for destructive-op confirmation. Without `-y` the task aborts mid-run with:

```
task: Task "<name>" cancelled because it has a prompt and the environment is not a terminal. Use --yes (-y) to run anyway.
```

Default to `task -y <name>` from non-interactive contexts. The flag is harmless on tasks without prompts.

## Coding style

- Go: idiomatic, `gofmt`/`goimports` clean, `golangci-lint run` clean (v2 config).
- Errors are wrapped with `%w` and carry context.
- Logging via `log/slog` with the JSON handler. Never use `log.Printf`.
- No `panic` outside `main` boot unless explicitly justified in a comment.

## Structure

- `cmd/<binary>/main.go` — thin entry point. Parses env, wires deps, runs.
- `internal/<package>/` — implementation packages, never imported across `cmd/`.
- `infra/` — Terraform; one resource family per `<family>.tf` file.

## Planning

Implementation plans must be vertically sliced — each plan delivers an
end-to-end working increment (build → deploy → smoke-test → observe),
not a horizontal layer (all data models, then all APIs, then all UI).

A plan ships as ONE simple commit at the end, with a single imperative
commit message. No phase-by-phase commits, no `Co-Authored-By`, no
bulleted file-by-file summary in the body.

## Testing

- Table-driven tests via `t.Run`. Use `testing.T.Helper` in test helpers.
- Integration tests gated behind `//go:build integration` and run via `task test:integration`.

## Reading the design

- System reference: `docs/reference/system.md`
- Data paths: `docs/reference/data-paths.md`
- Ops shortcuts: `docs/reference/ops.md`
- Backlog (issues + milestones): [Kalshiflow Linear project](https://linear.app/holdlayer/project/kalshiflow-1e60addc8327). `docs/next-steps.md` is a pointer to it; pipeline-status snapshot lives there too.

