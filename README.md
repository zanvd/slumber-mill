# SlumberMill

A multi-tenant audio ingest pipeline in Go. The full README is still to be written;
[SPEC.md](SPEC.md) describes the design.

## Process-level checks

`make process-checks` runs `scripts/process-checks.sh`, which checks how `worker-encode` stops and crashes
under Compose while it is encoding:

1. `docker compose stop` lets it drain for the shutdown grace, then release the asset to `queued` with its attempts
   unchanged, and exit 0 within `stop_grace_period`.
2. After a SIGKILL and a restart once the encode lease has passed, the asset is reaped and processed as attempt 2,
   and the dead attempt's temp files are swept.
3. With a stop timeout below the shutdown grace, check 1 fails: Docker kills the worker mid-drain and the asset isn't
   released. That's why `stop_grace_period` is longer than the grace.
4. A second SIGTERM during the drain ends the worker at once.

The script needs only bash 5 and Docker, and takes 6–8 minutes, mostly encoding and waiting out the lease.
It isn't run in CI. It works on a Compose project of its own, `<name>-process-checks`, where `<name>` is
`COMPOSE_PROJECT_NAME` or `slumber-mill`, so it never touches your everyday stack. It builds and starts that project,
and when it ends takes it down with its volumes and the images it built. It mounts `var/out/process-checks/data`
as the data directory instead of `var/data` and publishes no port, so it can run beside a local stack.
Uploads go through a pinned `curlimages/curl` container on the project's network. The containers' logs are left in
`var/out/process-checks/compose.log`.

## MySQL connection limit

Compose starts MySQL with `--max-connections=400`; the server's default is 151.
Every process opens one pool of at most 25 connections (`MaxOpenConns 25`), and the worst case is that all of
them are full at the same time while the test suite runs against the same server:

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
