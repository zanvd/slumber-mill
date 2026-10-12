# CLAUDE.md

Rules for agents working in this repository. [SPEC.md](SPEC.md) is the source of truth for what to build;
this file says how to work. `§N` means a section of SPEC.md, and "pitfall N" an item in its §14.

## Project

SlumberMill is a multi-tenant audio ingest pipeline in Go. A tenant uploads a sleep story or soundscape;
the encode pool loudness-normalises it, renders two AAC renditions and verifies each one,
and the webhook pool notifies the tenant with a signed webhook.

- **Pinned stack (§3):** Go 1.27, Gin 1.12, GORM 1.31 with `gorm.io/driver/mysql` 1.6, MySQL 8.4,
  ffmpeg 6.1.1 from Ubuntu 24.04, Docker Compose v2, GitHub Actions. No other dependencies.
- **One binary, four subcommands** (`migrate`, `api`, `worker -pool encode|webhook`, `tenant create`),
  each run as its own Compose service (§4).
- **The build order is §15.**

## Roles

Work runs through one overseer, who delegates to implementers and reviewers.
Each role's instructions are in its own file under `.claude/agents/`; the rules in this file apply to all of them.

- **Overseer** ([overseer.md](.claude/agents/overseer.md)): the project manager. It plans the build order,
  delegates tasks and reviews, decides when a pull request is ready, and reports to the user. It writes no code.
  It's the default main session (`"agent": "overseer"` in [.claude/settings.json](.claude/settings.json));
  `claude --agent <name>` overrides that.
- **Implementer** ([implementer.md](.claude/agents/implementer.md)): owns one task, one branch and one pull request,
  in its own worktree.
- **Reviewer** ([reviewer.md](.claude/agents/reviewer.md)): checks one pull request against its brief, SPEC.md
  and this file, reruns the checks in its own worktree, and posts its findings on the pull request
  as review comments and suggestions. It changes no code.
- Implementers and reviewers report to the overseer, never to the user; the user sees their work on the pull request.
- **The user** decides everything under "What needs asking first", and is the only one who merges.

### What needs asking first

Go ahead without asking:

- Reading anything; running builds, vet, tests and `docker compose` against the local stack.
- Creating and editing files under `src/`, the `Dockerfile`, `compose.yaml`, `.github/`, `README.md`, a `Makefile`
  and the `var/**/.gitkeep` files.
- Adding the modules listed in §3 at their pinned versions.
- Committing to, rebasing and pushing your own branch, and opening a pull request for it.
- Posting on a pull request what your role's file allows.

These are the user's decisions; implementers return them to the overseer, which asks the user:

- **Any departure from SPEC.md,** including fixes to it that look obvious.
- **Any module not listed in §3,** test helpers and mocking libraries included.
- **Changing a pinned version:** Go, Gin, GORM, the driver, MySQL, base image digests or the ffmpeg packages.
  The measured figures in the spec depend on them.
- **Editing** `SPEC.md`, `CLAUDE.md`, anything under `.claude/`, `LICENSE`, `.gitignore` or `.editorconfig`.
- **Destructive operations:** `docker compose down -v`, removing volumes or `var/data`,
  or dropping any database the test suite didn't create.

Never:

- Commit or push to `master`, or touch another agent's branch.
- Merge, approve or enable auto-merge on a pull request.
- Force-push, except `git push --force-with-lease` to your own branch after rebasing it.
- Install Go, ffmpeg, MySQL or anything else on the host, or run the project's Go there (see "Commands").
- Delete, skip or loosen a test to get a green run. Integration tests fail when MySQL or ffmpeg is missing;
  they never skip (§13).
- Put a real secret in the repository. The only key in it is the Compose key marked dev-only (§8).
- Report a check as passing without having run it.

### When the spec and reality disagree

The spec pins versions and quotes measurements from a prototype.
If a pinned version doesn't exist, an API behaves differently from what the spec says, a measurement doesn't reproduce,
or two sections contradict each other: stop, show the evidence (the command and its output),
propose the smallest change, and wait. Once the user agrees, update SPEC.md in the same pull request as the code.

### Go newer than your training data

Go 1.25–1.27 added APIs the spec relies on: the standard library's `uuid` package with `uuid.NewV7()`,
container-aware `GOMAXPROCS`, `sync.WaitGroup.Go`, `errors.AsType`, `testing/synctest` with `synctest.Sleep`,
`httptest.NewTestServer` and the goroutine leak profile.
Use them, and don't substitute third-party packages (no `github.com/google/uuid`).
When unsure of a signature, run `go doc` in the `dev` container instead of guessing.

## Commands

Everything runs in Docker (§12). Go commands run in the `dev` service, whose working directory is the module (`/src`),
because it carries the pinned Go, cgo for `-race` and the same ffmpeg build as the encode worker.
A Go toolchain on the host is not the project's toolchain.

```sh
docker compose up -d --build                                    # MySQL, migrations, API and both pools
docker compose run --rm api tenant create -slug demo            # prints the API key once
docker compose run --rm --no-deps dev go test -race ./...       # unit tests only; MySQL isn't started
docker compose run --rm dev                                     # whole suite: go test -race -tags integration ./...
docker compose run --rm dev go test -race -tags integration -run TestName ./asset/   # one test
docker compose run --rm dev go vet -tags integration ./...
docker compose run --rm --no-deps dev gofmt -l .
docker compose run --rm --no-deps dev go mod tidy
docker compose run --rm dev go test -tags integration -coverprofile=/out/cover.out ./...
```

- Until the `dev` service exists (build-order step 2), run Go in a throwaway `golang:1.27` container with `src/` mounted.
- Each worktree is its own Compose project, named after its directory, so parallel agents don't share MySQL.
  Only one of them at a time runs the full stack with `up`, because `api` publishes port 8080.
- GitHub work goes through `git` and the `gh` CLI on the host, as the bot (see "Identity").
- A Makefile may wrap these, as long as each target stays a single `docker compose` command (§12).
- Test and tool output goes to `/out` (`var/out` on the host). Nothing the containers write lands in `src/`.

## Definition of done

A change is done when all of these hold in the `dev` service:

- [ ] `gofmt -l .` prints nothing, and `go vet -tags integration ./...` is clean.
- [ ] `go test -race ./...` passes with `--no-deps`, so the unit tier needs nothing but Go.
- [ ] The whole suite passes: `go test -race -tags integration ./...`.
- [ ] New behaviour is tested at the right tier, and the §13 "What the tests must show" items for the area are covered.
- [ ] Changes to shutdown, signals or Compose stop settings also pass the process-level checks.
- [ ] `go mod tidy` changes nothing, and `go.mod` and `go.sum` change together.
- [ ] The §16 acceptance criteria the change touches still hold.
- [ ] README or SPEC.md is updated if behaviour a tenant or operator sees has changed.

If a check couldn't run, say which one and why.

## Hard rules

Condensed from §3–§12 and §16; read the cited section before working in its area.
Breaking one of these is a bug even when the tests pass.

### Layout and layers (§4)

- `src/` is the module `github.com/zanvd/slumber-mill/src`, with `go 1.27.0` in `go.mod`. No `internal/`.
- Transport (`httpsrv/`, the CLI) → service (`service.go`, `processor.go`, `worker/`) → repository
  (`repository.go`, `db/`), with adapters beside the repositories (`audio/`, `storage/`, `webhook/client.go`).
- `gorm.io` is imported only by `db/` and the `repository.go` files, and Gin only by `httpsrv/`.
- Wiring happens only in `cmd/slumber-mill/main.go`. No package-level state, no wiring in `init`.
- Interfaces are declared, unexported, by the package that uses them, with only the methods it calls.
  Constructors take interfaces and return concrete types.
- Repository methods and GORM models are unexported, and models convert to domain types at the boundary,
  so no domain type carries GORM tags or a `db.ID`.
- A plain `context.Context` crosses layers, never `*gin.Context`: handlers pass `c.Request.Context()` (pitfall 1).
- Errors are translated at each boundary. Repositories map GORM errors to their package's errors and wrap the rest
  so driver errors are out of reach of `errors.Is` and `errors.As`; `httpsrv` maps service errors in one function.
- API responses and webhook payloads use the `apiv1` types.

### Transactions and data (§4, §5, §11)

- A transaction lives inside one repository method. No service opens one, and no transaction handle,
  `*gorm.DB` or transaction-carrying context exists above the repositories.
- Effects on another domain go through the `asset_events` outbox; reads from another domain are service calls.
- Every tenant-owned query filters on `tenant_id`. Another tenant's asset is the same 404 as a missing one.
- Public and event IDs are UUIDv7 in `BINARY(16)` through `db.ID`; the `BIGINT` key stays internal.
- Closed sets are `ENUM` or `SET`, and new members are only ever appended.
- No `AutoMigrate`. Schema changes are numbered SQL files in `db/migrations/`: one logical, re-runnable change each,
  with explicit `ALGORITHM` and `LOCK`. A migration already on `master` is never edited.

### GORM (§3)

- `SkipDefaultTransaction` and `TranslateError` are on, and every call uses `WithContext(ctx)`.
- `CreatedAt` and `UpdatedAt` are tagged `gorm:"->"`; times come from MySQL's `NOW(6)`, never the Go clock.
- Updates use maps, with `gorm.Expr` for arithmetic and `NOW(6)`; never struct `Updates` or `Save` (pitfall 7).
- Single-row reads use `Take`, upserts use `clause.OnConflict`, and fenced writes check `RowsAffected` (pitfall 12).

### Leasing and workers (§6)

- Claims run in READ COMMITTED with `FOR UPDATE SKIP LOCKED` (pitfall 11).
- Every write after a claim is fenced on `id`, the leased state and `attempts`; zero rows means `ErrLeaseLost`.
- The lease SQL exists once, in `db/`, and the runtime once, in `worker/`.
- Outcome order: lease lost → discard; success → done; cancelled or draining → release; otherwise → fail.
- Encode concurrency comes from `runtime.GOMAXPROCS(0)`, never `runtime.NumCPU()`.
  The encode and webhook pools stay separate.

### Audio (§7)

- Measure and normalise per output channel layout, after the downmix, and verify every rendition:
  within ±1 LU of target and true peak at or below the ceiling.
- Check ffmpeg's exit status before parsing any report (pitfall 21); pass 2 runs at `-loglevel info`.
- Child processes use `exec.CommandContext`, `WaitDelay` and `Setpgid` (pitfall 24).
- Rejections are permanent and carry a tenant-safe message; ffmpeg's stderr goes only to logs and `last_error`.
- Changing encoder flags or the ffmpeg build means rerunning the audio tests.

### Security (§5, §8, §9)

- API keys are stored only as SHA-256, and webhook secrets only as AES-256-GCM ciphertext bound to their row.
- Secrets never reach logs; an opened secret is a `secret.Value`. `tenant create` prints them once, on stdout.
- Only `api` and `worker-webhook` receive `SLUMBER_MILL_SECRET_KEYS`.
- The webhook client checks the dialled IP with `IsPublic`, sets `Proxy: nil` and follows no redirects.
- Gin's trusted proxies are always set explicitly (pitfall 2).
- `interpolateParams=true` is refused, and `multiStatements=true` is allowed only on the migration pool.

## Testing (§13)

- **Unit:** untagged `_test.go` files. Services run against hand-written fakes of the interfaces they declare,
  and handlers against fake services.
- **Integration:** `//go:build integration`, against a throwaway MySQL database per test (`testutil/`)
  and the real ffmpeg. Repositories and `audio` are tested only here.
- **Process-level:** scripted against Compose.
- Tests are table-driven with subtests, and deterministic: fixed noise seeds, `synctest` for timing logic,
  and expired leases made by updating the column. No `time.Sleep` to wait for something to happen.
  Anything touching MySQL runs on real time, outside `synctest`.
- Everything runs with `-race`.

## Code style

- `gofmt`, standard Go naming, and the names the spec uses for packages, functions, env vars, tables and columns.
- Every file follows [.editorconfig](.editorconfig): UTF-8, LF line endings, a final newline, no trailing whitespace,
  and spaces for indentation except in Go files and Makefiles.
- Logging is `slog` JSON to stderr, through `logging.LoggerFromContext(ctx)`. Stdout is only for `tenant create`'s secrets.
- **Comments are kept to a minimum.** Write one only where the code doesn't make clear what it does
  and the comment adds real value. Don't restate the code, narrate the change or cite the spec.
  The same goes for doc comments: an exported name that says what it does needs none.

## Git and pull requests

### Branches

- All work happens on a branch. Nothing is committed to `master` directly.
- Names are `feat/<topic>` or `fix/<topic>`, where `<topic>` is a few lower-case words joined by hyphens:
  `feat/lease-sql`, `feat/webhook-signing`, `fix/relay-lock-release`.
  `fix/` is for bug fixes; everything else, including build, CI, test and docs work, uses `feat/`.
- Branch from an up-to-date `origin/master`.
- Update a branch by rebasing it onto `origin/master`, never by merging `master` into it:
  before opening its pull request, and again when it conflicts or the overseer asks.
  Push the rebased branch with `git push --force-with-lease`.

### Identity

Agents act on GitHub as the app `zanvd-claude` (`zanvd-claude[bot]`), never as the user;
`~/.local/bin/claude-gh/README.md` describes the setup.

- `.claude/settings.json` makes the bot git's author and committer, sends git over HTTPS with the app's token,
  and turns off SSH and commit signing. A `SessionStart` hook puts the `gh` shim, which runs as the bot, first on `PATH`.
- If `gh` says it isn't logged in, the shim isn't on `PATH`: call `~/.local/bin/claude-gh/shim/gh` instead.
  That error is what stops `gh` from acting as the user, so never get past it any other way.
- Never change the identity or handle credentials: no `git config user.*` or `-c user.*`, no overriding `GIT_*`
  variables, no `gh auth login`, no `GH_TOKEN`, no running `gh-app-token`, no reading the app's private key.
- Commits are unsigned, and that's expected: squash merges are the only way into `master`, and GitHub signs those.

### Commits

- One logical change per commit: code, its tests and `go.sum` together, and each commit builds and passes the unit tests.
- Never bypass hooks with `--no-verify`.

### Commit messages

Commit messages follow Commitizen's conventional commits rules, the format `cz check` enforces:

```
<type>(<scope>)!: <subject>

<body>

<footer>
```

- **type** is required:

  | Type | For | Version bump |
  |---|---|---|
  | `feat` | new behaviour | minor |
  | `fix` | a bug fix | patch |
  | `refactor` | a code change that neither fixes a bug nor adds behaviour | patch |
  | `perf` | a performance improvement | patch |
  | `test` | tests only | none |
  | `docs` | README, SPEC.md, CLAUDE.md, comments only | none |
  | `style` | formatting only | none |
  | `build` | Dockerfile, compose.yaml, Makefile, Go modules | none |
  | `ci` | `.github/workflows/` | none |
  | `chore` | anything else, such as `.gitignore` | none |
  | `revert` | reverting an earlier commit | none |

  `bump` is reserved for `cz bump`.
- **scope** is optional: one lower-case word with no spaces. Use a package (`db`, `worker`, `tenant`, `asset`,
  `webhook`, `audio`, `storage`, `secret`, `apiv1`, `validation`, `logging`, `httpsrv`, `testutil`, `cmd`)
  or an area (`migrations`, `docker`, `compose`, `deps`, `spec`, `readme`, `git`).
  Leave it out rather than invent a vague one.
- **`!`** after the type or scope marks a breaking change, which bumps the major version.
- **subject:** imperative mood, starts with a lower-case letter, no trailing period;
  the whole header line stays within 72 characters.
- **body** (optional, after a blank line): why the change was made, wrapped at 72 characters,
  citing spec sections and pitfalls where they apply.
- **footer** (optional, after a blank line): `BREAKING CHANGE: <what breaks>` alongside the `!`,
  and trailers such as `Refs:` or `Co-Authored-By:`.
- **Breaking** here means a change to the HTTP API (§9), the webhook payload or signature (§8),
  an environment variable, a CLI flag, or a migration that older code can't run against.

Examples:

```
feat(worker): fence heartbeats on attempts
build(docker): pin ffmpeg packages to 7:6.1.1-3ubuntu5
test(webhook): cover NAT64 and IPv4-mapped forms in IsPublic
```

```
fix(db): force clientFoundRows in the DSN

A fenced update that matched its row but changed nothing returned zero
rows affected, which the worker read as a lost lease (pitfall 12).
```

### Pull requests

- The implementer opens one against `master` once the definition of done holds and the branch is rebased.
- **Title:** a commit header in the format above, such as `feat(worker): add lease runtime`.
  Squash is the only merge method, so the title becomes the commit header on `master`.
- **Description:** the task brief, the spec sections it covers, what changed, the checks that ran and their results,
  deviations from the spec, open questions and follow-ups.
- **Reviews** are posted by the reviewer as comment reviews, with inline comments and suggestions.
  The implementer fixes blocking comments with new commits on the same branch and replies on each thread;
  the reviewer resolves a thread once its finding is fixed.
- Every agent posts as the same bot, so each review, comment and reply starts with its author's role:
  `**[reviewer]**` or `**[implementer]**`.
- Only the user merges. No agent merges, approves or enables auto-merge.
- Rulesets on `master` enforce this: changes arrive only through pull requests, as squash merges,
  with every conversation resolved, and only the user can merge.
