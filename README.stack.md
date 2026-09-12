# price-streaming

Change-data-capture pipeline: PostgreSQL outbox table -> Debezium -> Redpanda (Kafka),
with schema managed by Liquibase.

## Stack

Start the infrastructure before doing anything else:

```bash
docker compose up -d
```

| Service       | URL / Port              | Notes                          |
|---------------|-------------------------|--------------------------------|
| PostgreSQL    | `localhost:5432`        | db `pricing`, `username`/`password` |
| Redpanda      | `localhost:19092`       | Kafka API                      |
| Redpanda Console | http://localhost:8082 | topic browser                  |
| Kafka Connect | http://localhost:8083   | Debezium REST API              |
| Debezium UI   | http://localhost:8081   |                                |
| Centrifugo    | http://localhost:8000   | admin/admin                    |

---

## Database migrations (Liquibase)

### Layout

```
src/main/resources/db/changelog/
├── db.changelog-master.yaml          # picks up every *.xml under changes/
└── changes/
    ├── 001-create-outboxevent-table.xml
    └── 002-create-price-configs-table.xml
```

`db.changelog-master.yaml` is on Spring Boot's default changelog path, so no
`spring.liquibase.change-log` property is needed. It uses `includeAll` with
`endsWithFilter: .xml`, so **a new migration is picked up simply by dropping an
`.xml` file into `changes/`**. Files are applied in alphabetical order — keep the
numeric prefix (`002-`, `003-`, …) so that order stays meaningful.

Changesets use native PostgreSQL DDL inside `<sql dbms="postgresql">` rather than
Liquibase's abstracted change types. This pins the exact types emitted (`jsonb`,
identity columns) at the cost of portability: these migrations are PostgreSQL-only.

### Applying migrations

Migrations run **automatically on application startup**:

```bash
./mvnw spring-boot:run
```

The app logs either `Running Changeset: …` or `Database is up to date, no changesets to execute`.

### Manual operations (Maven plugin)

For everything the app can't do at startup — rollback, dry runs, inspection — use
the `liquibase-maven-plugin`, configured in `pom.xml`. It targets
`localhost:5432/pricing`.

**Credentials come from the environment.** The plugin reads `POSTGRES_USER` and
`POSTGRES_PASSWORD` — the same variables `compose.yaml` and `application.yaml`
use. Export them once per shell before running any goal below:

```bash
export POSTGRES_USER=username
export POSTGRES_PASSWORD=password
```

Maven has no default syntax for `${env.*}`, so an unset variable is **not**
silently replaced with a fallback — the goal fails with:

```
Connection could not be created to jdbc:postgresql://localhost:5432/pricing …
The server requested SCRAM-based authentication, but the password is an empty string.
```

If you see that, the environment isn't set. You can also pass credentials
per-invocation, which takes precedence over the environment:

```bash
./mvnw liquibase:status -Dliquibase.username=… -Dliquibase.password=…
```

The URL is still hardcoded in `pom.xml`; override it with `-Dliquibase.url=…` to
point a goal at another database.

> The plugin resolves the changelog from the **build classpath**, so run
> `./mvnw process-resources` after editing a changelog to refresh `target/classes`
> before invoking any plugin goal. This is what keeps the plugin and the
> application in agreement about which changesets have run.

```bash
# What would run against the database?
./mvnw liquibase:status

# Apply pending changesets without starting the app
./mvnw liquibase:update

# Generate the SQL instead of executing it (review before touching a real database).
# Writes to target/liquibase/migrate.sql -- it does NOT print to stdout.
./mvnw liquibase:updateSQL
```

### Rollback

Every changeset defines an explicit `<rollback>` block, so all of these work:

```bash
# Roll back the last N changesets
./mvnw liquibase:rollback -Dliquibase.rollbackCount=1

# Roll back to a tag
./mvnw liquibase:rollback -Dliquibase.rollbackTag=v1.0

# Roll back everything applied after a date (yyyy-MM-dd'T'HH:mm:ss)
./mvnw liquibase:rollback -Dliquibase.rollbackDate=2026-09-12T00:00:00

# Preview the rollback SQL without executing it
# (also written to target/liquibase/migrate.sql, database left untouched)
./mvnw liquibase:rollbackSQL -Dliquibase.rollbackCount=1
```

`rollbackCount=1` rolls back the **most recent** changeset — currently
`002-create-price-configs-table`, which drops `price_configs` and removes its row
from `databasechangelog`, leaving `outboxevent` untouched. Re-apply with
`./mvnw liquibase:update`.

Tag the current state first if you want a named rollback target:

```bash
./mvnw liquibase:tag -Dliquibase.tag=v1.0
```

### Editing an already-applied changeset

Liquibase checksums each changeset. Changing one that has already run makes the
next startup fail with a validation error. On a throwaway dev database:

```bash
./mvnw liquibase:rollback -Dliquibase.rollbackCount=1   # before editing, if it has a rollback
# or, to reset completely:
docker exec postgres psql -U username -d pricing \
  -c "DROP TABLE IF EXISTS outboxevent, price_configs, databasechangelog, databasechangelog_lock CASCADE;"
```

Where you can't drop tables, use `./mvnw liquibase:clearCheckSums` or add a
`validCheckSum` to the changeset. Prefer adding a **new** changeset over editing
an applied one.

---

## Debezium outbox connector

`etl/outbox-connector.json` configures a PostgreSQL source connector that captures
`public.outboxevent` and applies the **outbox event routing** SMT
(`io.debezium.transforms.outbox.EventRouter`):

| Outbox column   | Becomes                                            |
|-----------------|----------------------------------------------------|
| `event_id`      | the `id` Kafka header (`table.field.event.id`)     |
| `aggregateid`   | the message key                                     |
| `aggregatetype` | the topic: `<aggregatetype>.events`                 |
| `type`          | *not emitted by default* — see below                |
| `payload`       | the message value, expanded from `jsonb` to a struct |
| `id` (bigint)   | dropped — the PK is not propagated                  |

So a row with `aggregatetype = 'Trade'` is routed to the topic `Trade.events`,
and the only header on the message is `id`, carrying the row's `event_id`.

`table.field.event.type` merely tells the router which column holds the event
type; it does **not** put it on the message. To surface it as a header, add:

```json
"transforms.outbox.table.fields.additional.placement": "type:header:eventType"
```

Because `table.expand.json.payload` is `true`, the `jsonb` payload arrives as a
typed Connect struct rather than an escaped JSON string — so
`{"price": 42000, "qty": 1.5}` yields fields `price` (int32) and `qty` (double).
Note the schema is inferred per message, so types can vary between rows.

The router only handles **inserts** — the outbox table is append-only. Updates and
deletes are dropped with a warning.

### Create

```bash
curl -i -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @etl/outbox-connector.json
```

Validate the config *before* creating it (returns `error_count: 0` when valid):

```bash
curl -s -X PUT http://localhost:8083/connector-plugins/io.debezium.connector.postgresql.PostgresConnector/config/validate \
  -H "Content-Type: application/json" \
  --data @etl/outbox-connector.json
```

### Inspect

```bash
curl -s http://localhost:8083/connectors                             # list
curl -s http://localhost:8083/connectors/price-source-connector/status
curl -s http://localhost:8083/connectors/price-source-connector/config
```

### Update

`PUT` to `/config` is an upsert — it creates the connector if absent and otherwise
applies the new config in place. Note it takes the **inner `config` object only**,
not the full file (which is wrapped in `{"name": …, "config": {…}}`):

```bash
jq '.config' etl/outbox-connector.json | curl -i -X PUT \
  http://localhost:8083/connectors/price-source-connector/config \
  -H "Content-Type: application/json" --data @-
```

Without `jq`:

```bash
python3 -c "import json;print(json.dumps(json.load(open('etl/outbox-connector.json'))['config']))" \
  | curl -i -X PUT http://localhost:8083/connectors/price-source-connector/config \
    -H "Content-Type: application/json" --data @-
```

Restart or pause a connector without changing its config:

```bash
curl -i -X POST http://localhost:8083/connectors/price-source-connector/restart
curl -i -X PUT  http://localhost:8083/connectors/price-source-connector/pause
curl -i -X PUT  http://localhost:8083/connectors/price-source-connector/resume
```

### Delete

```bash
curl -i -X DELETE http://localhost:8083/connectors/price-source-connector
```

Deleting the connector does **not** drop its PostgreSQL replication slot, which
will keep pinning WAL and consuming disk. To remove it fully:

```bash
docker exec postgres psql -U username -d pricing \
  -c "SELECT pg_drop_replication_slot('debezium_slot');" \
  -c "DROP PUBLICATION IF EXISTS dbz_publication;"
```

### End-to-end check

```bash
# 1. Create the connector, then write an outbox row
docker exec postgres psql -U username -d pricing -c \
  "INSERT INTO outboxevent (event_id, aggregatetype, aggregateid, type, payload)
   VALUES (gen_random_uuid(), 'Trade', 'BTC-USDT', 'TradeExecuted', '{\"price\": 42000}');"

# 2. It should appear on Trade.events, keyed by BTC-USDT,
#    with an `id` header equal to the row's event_id
docker exec kafka rpk topic consume Trade.events --offset start --num 1 \
  -f 'HEADERS: %h{%k=%v }\nKEY: %k\nVALUE: %v\n'
```
