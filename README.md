# SlumberMill

A multi-tenant audio ingest pipeline in Go. The full README is still to be written;
[SPEC.md](SPEC.md) describes the design.

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
