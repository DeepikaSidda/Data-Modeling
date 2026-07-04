# Virtual Waiting Room (DynamoDB)

A DynamoDB-powered virtual waiting room that fairly queues up to **10,000,000 fans arriving within seconds** for a high-demand ticket sale. It assigns each fan a verifiable queue position, promotes fans from `WAITING` to `ELIGIBLE` in capacity-bounded batches, and serves low-latency real-time status to millions of concurrent pollers.

## Submission deliverables

The three required deliverables live in the [`submission/`](submission) folder:

| # | Deliverable | File |
|---|---|---|
| 1 | **NoSQL Workbench data model** — table, GSIs, key schemas, sample data | [submission/nosql-workbench-model.json](submission/nosql-workbench-model.json) |
| 2 | **Design document** — why each decision was made + trade-offs | [submission/design-document.md](submission/design-document.md) |
| 3 | **Access pattern matrix** — every pattern → table/index, key condition, filter expression | [submission/access-pattern-matrix.md](submission/access-pattern-matrix.md) |

A judge-facing overview is in [submission/README.md](submission/README.md).

## Repository layout

- [`waiting_room/`](waiting_room) — the Python package (pure-logic + DynamoDB data-access layers).
- [`tests/`](tests) — pytest unit, property-based (Hypothesis), and integration (moto) tests.
- [`infra/`](infra) — AWS CDK app (DynamoDB table, Lambdas, HTTP API, scheduled promoter).
- [`scripts/`](scripts) — live smoke, live API, and load-test harnesses.
- [`submission/`](submission) — the challenge deliverables.

## Design in brief

- **Single table `WaitingRoom`** (on-demand) with two GSIs and no LSIs.
- **`WaitingIndex`** (sparse GSI, `Waiting_Shard` / `Ordering_Key`) — front-of-line reads in position order; holds only `WAITING` entries.
- **`EligibilityIndex`** (GSI, `Elig_PK` / `Promotion_Time`) — capacity accounting, expiry sweep, status queries.
- **Fair ordering:** `Ordering_Key = <HLC sequence>#<server random tie-breaker>` (skew-tolerant, gaming-resistant).
- **Exactly-once admission** via `TransactWriteItems` + dedupe guard.
- **No over-promotion** via an atomic capacity counter.
- **Write sharding** with `Shard_Count = 4000` for the 10M-fan burst.


## Screenshots

> Place image files in a `screenshots/` folder (keep the filenames below, or update the paths). Each heading explains what the image shows.

### 1. NoSQL Workbench — Aggregate view (table + both GSIs)
The full model after import: the `WaitingRoom` table alongside `WaitingIndex` and `EligibilityIndex`.

![Aggregate view of the WaitingRoom model with both GSIs](screenshots/01-workbench-aggregate-view.png)

### 2. WaitingRoom table — items and attributes
The base table with all item types (queue entries, `ADMIT_COUNT`, dedupe guards, `CAPACITY`, `CONFIG`) and the `PK` / `SK` key schema.

![WaitingRoom base table data](screenshots/02-waitingroom-table.png)

### 3. WaitingIndex (sparse GSI)
Confirms the sparse index holds **only `WAITING`** entries — promoted entries are evicted when `Waiting_Shard` is removed.

![WaitingIndex sparse GSI](screenshots/03-waiting-index.png)

### 4. EligibilityIndex (GSI)
The status-partitioned index (`Elig_PK` / `Promotion_Time`) showing `ELIGIBLE` and `ACTIVE` entries.

![EligibilityIndex GSI](screenshots/04-eligibility-index.png)


### 8. Load test results
Bounded burst load test against real DynamoDB: exactly-once admissions, balanced shard distribution, and shard-count extrapolation to the full 10M burst.

![Load test output](screenshots/08-load-test.png)
