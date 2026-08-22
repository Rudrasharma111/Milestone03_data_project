# Data Governance — Purpose &amp; Approach

## Why metadata and governance matter here

A BigQuery warehouse full of tables is only useful if people can trust
and find the data. Metadata and governance solve two different problems:

- **Metadata** answers "what is this, and where did it come from?" —
  column descriptions, data types, source system, last-refreshed time,
  and lineage (which upstream tables fed this one). Without it, every
  analyst has to reverse-engineer the pipeline before trusting a number.
- **Governance** answers "who is allowed to see what?" — row-level
  security restricts *which rows* a user can see (e.g. an analyst only
  sees their own warehouse region's orders), and column-level security
  restricts *which columns* they can see (e.g. hiding `revenue` and
  `unit_price` from anyone without an explicit financial-data role).

Together, they let more people query the warehouse directly and safely,
instead of every request being funneled through one gatekeeper.

## How this project implements it

| Concern | Mechanism | Where |
|---|---|---|
| Dataset/table registration | Dataplex lake + zones (raw / curated) | `docs/gcp_commands_used.md` §5 |
| Column descriptions, source tracking | Data Catalog auto-harvests BigQuery schema; dbt `schema.yml` documents each model | `dbt_project/models/marts/schema.yml` |
| Lineage | dbt's `ref()`/`source()` graph, viewable via `dbt docs generate` | `docs/gcp_commands_used.md` §7c |
| Row-level access control | BigQuery row access policy on `fact_orders`, filtered by `warehouse_region` | `sql/03_row_level_security.sql` |
| Column-level access control | Data Catalog policy tags on `revenue`, `unit_price`, `list_price`, `profit_margin_pct` | `docs/gcp_commands_used.md` §7b |
| Data quality visibility | Malformed records routed to `orders_batch_errors` / `orders_stream_errors` instead of silently dropped | `ingestion/batch_ingest.py`, `ingestion/dataflow_pipeline.py` |

## Practical example from this project

An analyst in the West Hub warehouse region is granted access to
`reporting.fact_orders`. The row access policy means their queries only
ever return West Hub rows — they cannot see East Hub or South Hub data
even if they write `SELECT *`. If that same analyst has not been granted
the `Sensitive_Financial` policy tag role, the `revenue` and `unit_price`
columns return `NULL` for every row they can otherwise see. Both controls
apply independently and simultaneously — this is what "RLS and CLS
together" means in practice.

## Reliability: idempotency, job restarts, and late-arriving data

Two failure scenarios that are easy to overlook in a streaming pipeline,
and how this project handles each one.

### 1. What happens if the Dataflow job is stopped and restarted mid-flight?

`ParseAndValidate -> WindowIntoFixed -> KeyByOrderId -> GroupByKey ->
KeepFirstPerKey` dedupes `order_id` **within a single job run** — the
grouping state lives in that job's memory for the life of the job.

If the job is **cancelled** (not drained) while a message has already
been written to `orders_stream` but not yet acknowledged on the Pub/Sub
subscription, a **new** job that starts and re-subscribes has no memory
of the old job's state. Pub/Sub will redeliver that unacked message, and
the new job — having never seen it before — will write it again. This is
a real, demonstrable gap in the in-pipeline dedup, not a hypothetical one.

**Mitigation:** `sql/05_stream_dedup_reconciliation.sql` is a periodic
safety-net query (same pattern as the batch `MERGE`) that finds any
`order_id` with more than one row in `orders_stream` and keeps only the
earliest `ingestion_timestamp`. Run it after any job restart, or on a
schedule via BigQuery scheduled queries, so cross-restart duplicates
never make it into the reporting layer.

**How to prove it:** start the job, publish a batch, cancel the job
*before* all messages are acked (check the subscription's unacked count
in the Pub/Sub console), restart the job, let it finish, then run
`sql/05_stream_dedup_reconciliation.sql` step 1 — any duplicates it finds
are exactly the messages that were re-delivered across the restart.

### 2. What happens to data that arrives after the watermark has passed?

The window is `FixedWindows(60s)` with `allowed_lateness=300s` (5 min)
and a late trigger that fires immediately (`AfterCount(1)`) on any late
element. Anything arriving **within** that 5-minute grace period is still
processed normally and lands in `orders_stream`.

By default, Beam/Dataflow **silently drops** anything arriving *after*
`window_end + allowed_lateness` — there is no built-in signal, log line,
or error row for this; it just disappears. To avoid that silent data
loss, `ParseAndValidate` in `dataflow_pipeline.py` checks each event's
lateness *before* windowing (event_timestamp vs. current time) and, if
it exceeds `window + allowed_lateness`, routes the record into
`orders_stream_errors` with a clear `error_reason` instead of letting
the window drop it unseen. Every message therefore always ends up in
exactly one of two places: `orders_stream` or `orders_stream_errors` —
never silently discarded.

**How to prove it:** set `STREAM_LATE_OFFSET_SECONDS` in `.env` to a
value larger than `window (60s) + allowed_lateness (300s)`, e.g. `500`,
and run `stream_publish.py`. Query `orders_stream_errors` afterwards —
the late records should appear there with an `error_reason` starting
with "event arrived too late to process".

