# SlumberMill

A multi-tenant audio ingest pipeline in Go. A tenant uploads a sleep story or soundscape;
the encode pool loudness-normalises it to the tenant's target, renders two AAC renditions and verifies each one,
and the webhook pool notifies the tenant with a signed webhook.

The scope is narrow on purpose: one upload path, two renditions, two event types.
The depth goes into what fails in production: concurrency on MySQL, tenant isolation, webhook delivery,
shutdown, and loudness that is verified rather than assumed.
[SPEC.md](SPEC.md) is the full design.

```mermaid
flowchart LR
    C[Tenant app] -- "POST /v1/assets (multipart)" --> A[api]
    A -- "stream + sha256" --> D[(disk: sources/)]
    A -- "insert asset (queued)" --> M[(MySQL)]
    E[worker: encode] -- "claim asset (SKIP LOCKED) + lease" --> M
    E -- "probe, measure, render, verify" --> F[ffmpeg]
    F --> R[(disk: renditions/)]
    E -- "tx: renditions + ready + event" --> M
    W[worker: webhook] -- "relay events, claim deliveries" --> M
    W -- "signed POST" --> H[Tenant webhook]
```

One binary with four subcommands. `migrate`, `api` and each worker pool run as their own Compose service;
`tenant create` runs as a one-off command in the `api` service:

```
slumber-mill migrate                 apply database migrations
slumber-mill api                     serve the HTTP API
slumber-mill worker -pool P          run a worker pool: encode or webhook
slumber-mill tenant create -slug S [-webhook-url U] [-target-lufs F]
```

## Quick start

The host needs Docker with Compose v2 and nothing else; `make` is optional. On Linux, if your user ID isn't 1000,
put `APP_UID=` followed by the output of `id -u` in a gitignored `.env` first: the containers write to `var/` as
that user.

```sh
docker compose up -d --build                               # MySQL, migrations, API and both pools
docker compose run --rm api tenant create -slug demo       # prints the API key once
docker compose run --rm --no-deps dev go test -race ./...  # unit tests only; MySQL isn't started
docker compose run --rm dev                                # the whole suite against the Compose MySQL
docker compose run --rm dev go vet -tags integration ./...
docker compose run --rm dev go test -tags integration -coverprofile=/out/cover.out ./...
```

A Makefile wraps these: `make up`, `make tenant SLUG=demo`, `make test-unit`, `make test`, `make vet`, `make fmt`,
`make tidy`, `make cover`, `make down` and `make process-checks`.

Export the key from the `api_key:` line that `tenant create` printed as `API_KEY`, upload a file, set `ASSET_ID` to
the `id` field of the 202 response, then poll until the asset's `status` is `ready` and download a rendition
(`aac_128k` or `aac_64k`):

```sh
export API_KEY=sm_...
curl -H "Authorization: Bearer $API_KEY" -F title=Rain -F file=@rain.wav http://127.0.0.1:8080/v1/assets
export ASSET_ID=...
curl -H "Authorization: Bearer $API_KEY" http://127.0.0.1:8080/v1/assets/$ASSET_ID
curl -H "Authorization: Bearer $API_KEY" -o rain-128k.m4a http://127.0.0.1:8080/v1/assets/$ASSET_ID/renditions/aac_128k
```

The API listens on `:8080` inside its container, and Compose publishes that port on `127.0.0.1` only, so the local
stack isn't open to the network. Uploads and renditions live in `var/data`, so they can be played from there.

### HTTP API

All `/v1` routes need `Authorization: Bearer sm_...`.

| Method and path | Behaviour |
|---|---|
| `POST /v1/assets` | Multipart `title` and `file`; 202 with the asset and a `Location` header |
| `GET /v1/assets/:id` | The asset; 404 for a missing one and for another tenant's, which look the same |
| `GET /v1/assets?limit=&cursor=` | Newest first; `limit` 1-100, default 20; `data` and `next_cursor` |
| `GET /v1/assets/:id/renditions/:profile` | The file, with Range support |
| `GET /healthz` | Liveness: always 200 |
| `GET /readyz` | Readiness: a database ping; 503 as soon as shutdown begins |

Errors look like `{"error": {"code": "not_found", "message": "no such asset"}}`.
`Idempotency-Key` (optional, printable ASCII, at most 255 bytes, compared byte for byte) makes a retried upload one
asset: the same key with the same file and title replays the asset with 202 and `Idempotent-Replayed: true`,
and the same key with a different upload is `422 idempotency_key_reused`.

### Webhooks

A tenant created with `-webhook-url` gets `asset.ready` and `asset.failed` events:
`{"id": "<uuid>", "type": "...", "created_at": "...", "data": <asset>}`, where `data` is the API's own asset shape.
The request is a POST with `SlumberMill-Event-Id` and `SlumberMill-Signature: t=<unix>,v1=<hex>`, where `v1` is
the HMAC-SHA256 of `<t>.<body>` under the tenant's webhook secret. Delivery is at-least-once and unordered,
so receivers dedupe on the event ID. A 2xx counts as delivered, 410 Gone stops retrying, and anything else,
redirects included, is retried until the delivery has had 12 attempts, with a backoff that starts at 30 seconds
and caps at 6 hours. The package `github.com/zanvd/slumber-mill/src/webhook` exports `Verify` for receivers;
it accepts several `v1` values so secrets can rotate.

## Operations

### Creating a tenant

`tenant create -slug S [-webhook-url U] [-target-lufs F]` prints the credentials on stdout, once and nowhere else:

```
api_key: sm_...
webhook_secret: whsec_...
```

The `webhook_secret` line appears only when the tenant has a `-webhook-url`. Only hashes and ciphertext are stored,
so if you lose them, they are gone. Logs go to stderr as JSON. `-target-lufs` defaults to -16 and must be from -70 to -5.
The command refuses a webhook URL whose host isn't public.

Exit codes, for every subcommand: 0 for success or help, 1 for a runtime or configuration error,
and 2 for a usage error, such as a missing `-slug`.

### Configuration

Environment variables, with the defaults used when one is unset:

| Variable | Default |
|---|---|
| `SLUMBER_MILL_DSN` | `slumber_mill:slumber_mill@tcp(mysql:3306)/slumber_mill` |
| `SLUMBER_MILL_DATA_DIR` | `/data` |
| `SLUMBER_MILL_HTTP_ADDR` | `:8080` |
| `SLUMBER_MILL_MAX_UPLOAD_BYTES` | `2147483648` |
| `SLUMBER_MILL_MAX_UPLOADS_PER_TENANT` | `4` |
| `SLUMBER_MILL_ENCODE_CONCURRENCY` | `runtime.GOMAXPROCS(0)`, which follows the container's CPU limit |
| `SLUMBER_MILL_WEBHOOK_CONCURRENCY` | `8` |
| `SLUMBER_MILL_ENCODE_LEASE` | `2m` |
| `SLUMBER_MILL_SHUTDOWN_GRACE` | `30s` |
| `SLUMBER_MILL_DRAIN_DELAY` | `10s` (API: readiness fails, still serving) |
| `SLUMBER_MILL_TRUSTED_PROXIES` | empty: trust none |
| `SLUMBER_MILL_WEBHOOK_ALLOW_PRIVATE` | `false`; `true` in the local Compose stack |
| `SLUMBER_MILL_SECRET_KEYS` | none; `<id>:<base64 key>[,...]`, required by `tenant create` and the webhook pool |

- **Leases.** The encode lease is `SLUMBER_MILL_ENCODE_LEASE`, default 2 minutes, with a 1 minute minimum: a shorter
  value is a configuration error and the process exits 1. The `api` and both pools read it, because the startup
  sweep of `tmp/` removes files older than the lease. The webhook lease is fixed at 1 minute.
  A worker heartbeats every third of its lease.
- **Retention.** The webhook pool deletes deliveries in the `delivered` state 30 days after delivery and relayed
  events 30 days after relay, in small batches. The 30 days are fixed, not configurable. It runs at startup and then hourly.
  Assets and renditions are never deleted.
- **The secret key.** Only `api` (which runs `tenant create`) and `worker-webhook` get `SLUMBER_MILL_SECRET_KEYS`.
  Compose sets a fixed key marked dev-only; production reads it from a secret manager.

### Shutdown and crashes

- `docker compose stop` sends SIGTERM. The encode pool stops claiming, gives the encodes in flight
  `SLUMBER_MILL_SHUTDOWN_GRACE` to finish, then cancels them and releases the asset to `queued` with its attempts
  unchanged. A second SIGTERM ends the process at once.
- `stop_grace_period` is 45 seconds in `compose.yaml`, and must stay above the grace: Docker's default of 10 seconds
  would kill every worker mid-encode.
- The API fails `/readyz` on SIGTERM, keeps serving for `SLUMBER_MILL_DRAIN_DELAY`, then shuts down within the grace,
  and exits 0 even when the grace runs out.
- A signal before the API listens ends it with exit 0 and the log line `stopped before serving HTTP`.
- After a SIGKILL or a crash, the reaper requeues the asset once its lease has expired and counts the attempt.
  The dead attempt's temp files wait for the next startup sweep.
- Outside containers, run workers under systemd with `KillMode=mixed` and a `TimeoutStopSec` above the grace.
  A worker killed outright orphans ffmpeg there; in a container the kernel kills it with the container.

### Process-level checks

`make process-checks` runs `scripts/process-checks.sh`, which checks how `worker-encode` stops and crashes
under Compose while it is encoding:

1. `docker compose stop` lets it drain for the shutdown grace, then release the asset to `queued` with its attempts
   unchanged, and exit 0 within `stop_grace_period`.
2. After a SIGKILL and a restart once the encode lease has passed, the asset is reaped and processed as attempt 2,
   and the dead attempt's temp files are swept.
3. With a stop timeout below the shutdown grace, check 1 fails: Docker kills the worker mid-drain and the asset isn't
   released. That's why `stop_grace_period` is longer than the grace.
4. A second SIGTERM during the drain ends the worker at once.

The script needs only bash 5 and Docker, and takes 6-8 minutes, mostly encoding and waiting out the lease.
It isn't run in CI. It works on a Compose project of its own, `<name>-process-checks`, where `<name>` is
`COMPOSE_PROJECT_NAME` or `slumber-mill`, so it never touches your everyday stack. It builds and starts that project,
and when it ends takes it down with its volumes and the images it built. It mounts `var/out/process-checks/data`
as the data directory instead of `var/data` and publishes no port, so it can run beside a local stack.
Uploads go through a pinned `curlimages/curl` container on the project's network. The containers' logs are left in
`var/out/process-checks/compose.log`.

### Full-stack soak

`bash scripts/soak.sh` runs the whole stack under load while it breaks parts of it. It isn't run in CI and has no
`make` target. `worker-encode` and `worker-webhook` run two replicas each, so each table has two reapers, and the
encode lease is its 1 minute minimum. A driver, the `soak` service in `scripts/soak.compose.yaml`, runs
`TestFullStackSoak` (build tag `soak`) from the `dev` image on the project's network. The script creates five
tenants with `tenant create` in the `api` service. Four have webhooks pointing at the driver's receiver; the fifth
has none, and their targets range from -16 to -23 LUFS. A higher target, such as -14, would get most of the noise
fixtures rejected. Under the -4.5 dBTP pre-encode peak, loudnorm can't reach the target in linear mode, so it falls
back to dynamic mode and lands more than 1 LU short.

- **Uploads.** Ten uploaders go through the API for `SLUMBER_MILL_SOAK_DURATION` (default `10m`), and pause while
  12 assets are unfinished. They send 5-20 second stereo, mono and 5.1 noise fixtures from fixed seeds, an
  occasional 60 second thunder fixture, and text, silent and 1 second files, which are rejected. Every upload has an
  `Idempotency-Key`: 10% are replayed, 5% are sent twice at once, and 5% reuse their key for another file, which
  must be refused with 422. Connection errors and 429s are retried with the same key.
- **The receiver** verifies every signature. Its answer depends on the event ID: most events get a 2xx, some
  arrive slowly, and others get a 500 or a 302 once, two 503s, or a hang past the client's 15 second timeout.
  4% get a 410.
- **Failures.** Every `SLUMBER_MILL_SOAK_CHAOS_EVERY` seconds (default 45) the script injects one failure, taking
  them in turn. It sends SIGKILL to an encode worker or a webhook worker and restarts it 5-20 seconds later; that
  leaves leases for the other replica to reap, and a killed webhook worker that holds the relay lock loses it with
  its session. Or it runs `docker stop` on a webhook worker or the API, then starts it again.
- **The checks.** When the uploads stop, the driver waits up to `SLUMBER_MILL_SOAK_DRAIN_TIMEOUT` (default `15m`)
  for every asset and delivery to finish and every event to be relayed. Then it checks these invariants:
  - the run did enough for the rest to mean something: every kind of upload, a ready asset, a failure with each
    rejection message, a download, a retried delivery and a delivery failed on a 410;
  - every asset is one of the soak's uploads, with one asset per idempotency key;
  - every finished asset has exactly one event matching its state, and every event has exactly one delivery,
    or none for the tenant without a webhook;
  - only `ready` assets have renditions, two each;
  - nothing is left leased, and attempts stay within each table's maximum;
  - every rendition is within 1 LU of its target and at or below -1.5 dBTP, and its file has the recorded size
    and hash; a sample is also downloaded through the API, whole and by range;
  - rejected uploads failed with the tenant-safe message;
  - every encode attempt beyond an asset's first followed a reap: the script compares the extra attempts in the
    database with the reapers' logged requeues, and fails a run in which nothing was requeued;
  - every delivered event reached the receiver signed and was answered with a 2xx, and only events answered
    with a 410 failed;
  - every webhook's asset equals the API's final asset.

  The script also checks that each stopped container exited 0, or 137 after a SIGKILL. It fails on any deadlock or
  lock wait timeout in the services' logs, and on any ERROR line. GORM logs the losing insert of two racing uploads
  with the same key as a `query failed` WARN, which is normal. The script counts those lines, and they may number
  no more than the driver's concurrent pairs plus its retries after a connection error.

A run takes the duration plus 3-5 minutes to build, start and drain. It uses about 4 of the host's CPUs for
the two encode workers. The project is `<name>-soak`, where `<name>` is `COMPOSE_PROJECT_NAME` or, as Compose
itself defaults it, the checkout's directory name in lower case. That keeps soaks run from two checkouts apart. The
project uses `var/out/soak/data` as its data directory and publishes no port. At the end the script takes the project
down with its volumes and images. The driver's summary is left in `var/out/soak/summary.txt`, with its log in
`driver.log` and the services' logs in `compose.log`. Webhook delivery is at-least-once, so the summary counts the
duplicates the receiver saw, but they don't fail the soak.

### MySQL connection limit

Compose starts MySQL with `--max-connections=400`; the server's default is 151.
Each service process (`api`, `worker-encode`, `worker-webhook` and a one-off `tenant create`) opens one pool of at
most 25 connections (`MaxOpenConns 25`); a test package may hold two. The worst case is that all of them are full
at the same time while the test suite runs against the same server:

| Source | Connections |
|---|---|
| `api`, `worker-encode` and `worker-webhook`, 25 each | 75 |
| `migrate`, which holds one connection and exits before the others start | 1 |
| `docker compose run api tenant create`, a one-off process with its own pool | 25 |
| The `dev` suite: 5 packages with MySQL tests run in parallel, each holding up to two pools of 25 and one admin connection per throwaway database, 2 × (25 + 1) | 260 |
| The healthcheck's `mysqladmin ping` | 1 |
| Total | 362 |

The 38 connections left over cover one more test package. The figure is a ceiling, not a measurement:
pools open connections on demand, and two full `go test -race -tags integration ./...` runs alongside the
API peaked at 17 (`Max_used_connections`). When a new service or a test package that holds its own pool is added,
raise the limit by 25 for each pool.

## Design decisions

Each with its trade-off.

- **The asset is its own unit of work, and an outbox carries the webhooks.** An asset's `status` is its lease state,
  so an upload is one insert and finishing is one transaction in the asset repository, which also writes an
  `asset_events` row. The webhook pool's relay turns events into deliveries. No transaction spans two domains,
  and the cost is a relay hop: about 0.6 s on average and 1.2 s at p99 in the prototype, because the relay
  reads back to back until a batch comes back short instead of once a second. Exactly one relay runs at a time,
  elected with a MySQL `GET_LOCK`.
- **Leasing on MySQL, not Redis or SQS.** One fewer system to operate, and the work is created in the same insert
  or transaction as the data it belongs to. A claim is `SELECT ... FOR UPDATE SKIP LOCKED` in READ COMMITTED:
  under REPEATABLE READ the gap locks cost uploads a third of their throughput and tripled their p99 latency.
  Every write after a claim is fenced, meaning it matches only while the row's id, state and `attempts` are still
  the claimant's, so a worker that lost its lease changes nothing. Every time comes from `NOW(6)`, so clock skew between hosts can't distort a lease.
  A reaper requeues expired leases with a short delay, not the retry backoff. The price is that MySQL does the
  queueing: throughput is bounded by one database, and finished rows must stay cheap (they did, at a million
  finished rows per table).
- **Layers.** Gin (the HTTP framework) handlers, services and GORM (the Go database library) repositories are joined
  by interfaces that their consumers declare. Gin appears only in `httpsrv/` and GORM only in `db/` and the `repository.go` files, so a service tests against
  a hand-written fake and a handler against a fake service. The cost is a little ceremony: models convert to
  domain types at every boundary.
- **GORM rules that keep leasing correct.** Skip the default transaction, translate duplicate-key errors,
  pass the caller's context on every call, let MySQL write the timestamps, update with maps rather than structs
  (a struct silently drops zero values), and check `RowsAffected` on every fence, which needs `clientFoundRows=true`.
  No `AutoMigrate`: schema changes are numbered SQL files.
- **Per-rendition loudness, verified.** Loudness is measured in LUFS (loudness units relative to full scale), and a
  difference of one loudness unit is 1 LU. The true peak, in dBTP (decibels, true peak), is the highest level the
  decoded waveform reaches, including between samples. The mono downmix of an uncorrelated stereo recording lands
  about 3 LU below the stereo file, dual-mono content gains 3 dB of peak, and the AAC encoder moves both loudness
  and peak.
  A listener asleep with the volume set for one story notices a quiet or sudden file, so the encode pool measures
  each output channel layout after the downmix, normalises each with its own two-pass `loudnorm`, then re-measures
  every encoded file. Each must be within 1 LU of the tenant's target (default -16 LUFS) with a true peak at or below
  -1.5 dBTP, or the asset is rejected with the numbers. Each rendition stores its own figures.
  `loudnorm` can silently fall back to dynamic mode, which lifts a quiet rain bed when a thunder crack arrives,
  so the pool predicts the mode and publishes a `dynamic_normalization` warning. It also warns about
  `sudden_loud_event` (max momentary loudness more than 10 LU above integrated) and `source_may_clip`.
  Silent and shorter-than-3-second files are rejected. The cost is CPU: two measurement passes, a render and a
  verification pass per upload.
- **Tenant isolation, IDs and secrets.** Every tenant-owned query filters on `tenant_id`, and another tenant's asset
  is the same 404 as a missing one. Public and event IDs are UUIDv7 in `BINARY(16)`, which keeps inserts
  append-mostly at the cost of revealing the creation time. Idempotency keys use the `ascii_bin` collation, so `Upload-42` and
  `upload-42` are different keys; under MySQL's default collation they would collide. API keys are stored as SHA-256
  only. Webhook secrets can't be hashed, since every delivery signs with the secret itself, so they are stored as
  AES-256-GCM ciphertext bound to their row and tenant, under a key ring that lets a new key go first.
  Only `tenant create` and the webhook pool hold the key. Someone with both the database and that pool's
  environment can still forge events.
- **SSRF defences.** Tenants supply webhook URLs, so the client checks the IP it actually dials, after DNS
  resolution, and refuses anything that isn't public: private, loopback, link-local, carrier-grade NAT, documentation
  and 6to4 ranges, IPv4-mapped forms, and the well-known NAT64 prefixes. It uses no proxy and follows no redirects.
  `tenant create` checks the host too, but DNS can change afterwards, so the dial-time check is the real guard.
  A network-specific NAT64 prefix, which each network's operator chooses, can embed a private IPv4 address,
  and no fixed list covers it. If the webhook pool runs on such a network, block the private ranges at the network
  edge as well.
- **Shutdown: deploys don't use up retries or drop requests.** Workers stop claiming on SIGTERM, drain, and release
  unfinished work with its attempt returned. Work that fails while draining is released, not failed. ffmpeg runs in
  its own process group, so a terminal Ctrl-C reaches the worker first and the drain isn't cut short.
  The API fails readiness first and keeps serving for a drain delay, so the load balancer can take the node out.
  `stop_grace_period` is longer than the grace. Encoding and webhooks are separate pools, so a 3 ms webhook never
  waits behind a ten-minute encode.
- **One pinned ffmpeg build for tests and production.** The `dev` image and the encode worker's image share a base
  and the same pinned ffmpeg 6.1.1 packages, because the loudness figures depend on the encoder. Only the encode
  worker's image contains ffmpeg; the other runtime images are a static binary on distroless. Security fixes to
  ffmpeg need a deliberate bump and a rerun of the audio tests. Every ffmpeg run whitelists its input protocols and
  formats, so an uploaded playlist can't pull in other local files.

## Zero-downtime migrations

Migrations are numbered SQL files in `src/db/migrations/`, applied in order under a MySQL lock, so many instances
can start at once. MySQL DDL commits implicitly, so each file is one logical, re-runnable change, with `ALGORITHM`
and `LOCK` stated, so MySQL refuses an unsafe change instead of silently blocking writes. SPEC.md measured these on MySQL 8.0.46; the Compose stack runs MySQL 8.4:

| Change | Result |
|---|---|
| Add a nullable column | `ALGORITHM=INSTANT` |
| Append a value to the end of an ENUM | `ALGORITHM=INSTANT` |
| Insert an ENUM value mid-list | Table rebuild |
| Append a member to a SET (up to 8 members) | `ALGORITHM=INSTANT` |
| Grow a SET past 8 members | Table rebuild |
| Add a secondary index | `ALGORITHM=INPLACE, LOCK=NONE` |
| Change the collation of an indexed column | `ALGORITHM=COPY`, which blocks writes |

For a change that spans a deploy (not built, only written down here):

- **Expand, migrate, contract.** Add, dual-write, backfill, switch reads, stop writing, and drop in a later deploy.
  Never rename in one step.
- **A change that needs a COPY.** Build a new column and swap, or use gh-ost or pt-osc. Watch replica lag and
  metadata-lock waits, and set a low `lock_wait_timeout` on DDL sessions.
- **Backfills** run in keyset batches, with sleeps and a replica-lag check between them.
- **Examples.** Adding `language` to `assets` is INSTANT. A `rejected` status is appended to the ENUM, never inserted.
  The idempotency-key collation fix is done online with the new-column route above.

## Known limitations

- **No per-tenant fairness.** One tenant's 500-soundscape import delays everyone. The fix is per-tenant caps
  or a round-robin claim.
- **Local disk, and uploads through the API.** Uploads stream through the API to `var/data`, and a multi-GB upload
  on a slow link outlasts any shutdown grace. Production would have clients upload straight to object storage with
  a presigned URL (or tus), the API would only register the finished object, and renditions would sit behind signed
  CDN URLs, with the same write-then-commit discipline: the file lands before the row that points at it.
- **A retry during a still-streaming upload sends the bytes twice.** The idempotency row is written after the file
  is received, so the second upload loses the race only at the end. A reservation row at request start answering
  409 would close it.
- **AAC loop padding is left to the players.** AAC in MP4 isn't sample-accurate: a 480,000-sample tone decoded
  to 480,256 samples despite the MP4 edit list. A seamless soundscape can click at every seam unless the player
  honours gapless metadata or crossfades.
- **A source louder than 0 LUFS isn't handled.** loudnorm's `measured_I` option accepts -99 to 0, so pass 2 is
  expected to fail on such a source. That comes from the open questions of
  [pull request #11](https://github.com/zanvd/slumber-mill/pull/11), which added the audio package, and hasn't
  been re-run since. The failure is a plain ffmpeg exit, not a rejection: the encode pool retries it with backoff, and after three attempts the asset ends
  `failed` with the generic message "processing failed after repeated attempts", not a tenant-specific one.
  It's rare, and nothing checks for it up front.
- **Long noise-like soundscapes can still be rejected.** The 64k rendition rebuilds peaks the limiter removed,
  and its true peak can exceed -1.5 dBTP, more likely the longer the file.
- **Long bright content can miss the loudness tolerance at 64k.** White noise, hiss and rain measure about
  -17.0 LUFS for a -16 target, since the 64k encode has no `-cutoff`.
- **Bright content at loud targets can fail the peak ceiling.** Around -9 LUFS and above, the encoder's added
  peaks leave too little headroom.
- **Re-sealing webhook secrets under a new key is documented, not built.** A new key can go first in the key ring,
  and older ciphertexts still open; re-sealing the old rows and then dropping the old key is manual.
- **The webhook egress check and NAT64.** See "SSRF defences" above.
- **No metrics.** Logs only.

## Next steps

- Presigned or resumable uploads and signed CDN URLs.
- Prometheus metrics (backlog and age per pool, latency, webhook success) and alerts on failures.
- OpenTelemetry tracing.
- Webhook endpoint management and replay.
- Optional Ed25519 signatures, which remove per-tenant secrets altogether; HMAC is what most receivers verify today.
- Push notifications (OneSignal or FCM) and Stripe webhook intake on the same leasing and idempotency patterns.
- An admin UI.
