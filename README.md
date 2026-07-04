# Virtual Waiting Room — DynamoDB Data Model Submission

A DynamoDB-powered virtual waiting room that fairly queues up to **10,000,000 fans arriving within seconds**, assigns each a verifiable position, promotes fans from **waiting → eligible** in capacity-bounded batches, and serves low-latency status to millions of concurrent pollers.

---

## Submission deliverables (start here)

| # | Deliverable | File |
|---|---|---|
| 1 | **NoSQL Workbench data model** (`.json`) — table, GSIs, key schemas, sample data | [`nosql-workbench-model.json`](submission/nosql-workbench-model.json) |
| 2 | **Design document** — why each decision was made + trade-offs | [`design-document.md`](./design-document.md) |
| 3 | **Access pattern matrix** — every pattern → table/index, key condition, filter expression | [`access-pattern-matrix.md`](./access-pattern-matrix.md) |

> Import the model via **NoSQL Workbench → Import model → NoSQL Workbench model** and select `nosql-workbench-model.json`.

---

## The model at a glance

**Single table `WaitingRoom`** (on-demand billing) with an item-type taxonomy, **two GSIs, and zero LSIs** (the no-LSI choice is deliberate and justified in the design doc).

| Item type | PK | SK |
|---|---|---|
| Queue entry | `EVT#<Event_Id>#SH#<shard>` | `ENTRY#<Ordering_Key>` |
| Sharded admit counter | `EVT#<Event_Id>#SH#<shard>` | `ADMIT_COUNT` |
| Fan dedupe guard | `EVT#<Event_Id>#FAN#<Fan_Id>` | `ADMISSION` |
| Capacity counter | `EVT#<Event_Id>` | `CAPACITY` |
| Event config | `EVT#<Event_Id>` | `CONFIG` |

- **`WaitingIndex`** (sparse GSI): PK `Waiting_Shard`, SK `Ordering_Key` — present **only while `WAITING`**, so the promoter reads a shrinking front-of-line in order with no filter/scan.
- **`EligibilityIndex`** (GSI): PK `Elig_PK` (`EVT#<id>#<STATUS>`), SK `Promotion_Time` — capacity accounting, expiry sweep, status-by-status queries.

---

## How it meets the four judging criteria

### 1. Completeness
All required deliverables are present (above), and every challenge requirement is covered plus the stretch goal:
- Table modeling the queue (fan id, position, entry timestamp, eligibility status, batch assignment) — ✅
- Fair queue positioning with anti-gaming — ✅
- Batch promotion strategy — ✅
- Low-latency fan status queries — ✅
- **Stretch:** steady ~1,000 active purchasers with incremental refill — ✅

### 2. Data Model Correctness
Every access pattern resolves through a key condition on the table or a GSI — **no `Scan`, no `FilterExpression`** anywhere (see the matrix). Status selection is pushed into the key schema (sparse `WaitingIndex`, status-encoded `Elig_PK`); identity lookups use token-derived primary keys. The model imports and renders correctly in NoSQL Workbench, and the same schema was exercised end-to-end against real DynamoDB (see *Verification* below).

### 3. Scalability & Cost
- **Write burst:** partition key is `EVT#<Event_Id>#SH#<shard>`, sharded on `hash(Fan_Id)`. The binding constraint is the `WaitingIndex` GSI partition (1,000 WCU/s ceiling). The model ships **`Shard_Count = 4000`**, keeping each GSI partition ≈ 250 WCU/s at a 10M/10s burst.
- **Cost:** writes are cheap (~$60 one-time for a full 10M ingest); the real driver is polling reads, which is why status is served from cache/edge with an O(1) position estimate. Full analysis in the design doc.
- This is **empirically backed** — a load test confirmed shard balance and that 1,000 shards would exceed the ceiling (hence 4,000).

### 4. Design Rationale
The design document frames every major decision as **decision → alternatives considered → trade-off accepted**: single-table vs multi-table, write sharding & shard count, atomic capacity counter vs sharded counters, approximate vs exact position, HLC + random tie-breaker, sparse GSI vs status filtering, TTL vs authoritative expiry sweep, and the no-LSI choice.

---

## Key design decisions (one-liners)

- **Fair, gaming-proof ordering:** `Ordering_Key = <HLC sequence>#<server random tie-breaker>`. The Hybrid Logical Clock keeps ordering monotonic under clock skew; the server-side CSPRNG tie-breaker resolves same-instant arrivals unbiasably. All values are server-assigned — client input is ignored.
- **Exactly-once admission:** a `TransactWriteItems` writes the queue entry + a conditional dedupe guard atomically, so a `(Event_Id, Fan_Id)` can never yield two entries.
- **No over-promotion:** promotion reserves capacity against a single atomic `CAPACITY` counter (conditional update / optimistic version CAS), so concurrent promoters can never collectively exceed `Downstream_Capacity`.
- **Batch sizing:** `min(waiting, Max_Batch_Size, remaining_capacity)`, selected in position order via the sparse `WaitingIndex` (k-way merge across shards).
- **Low-latency status:** signed Entry_Token → single `GetItem` (no scan) + cached/approximate position + `Cache-Control: max-age` to absorb mass polling.
- **Active-pool regulation (stretch):** drives `Active_Count` toward ~1,000, refilling incrementally as slots free, always bounded by capacity.

---



> The implementation, tests, and infrastructure are bonus evidence. The three files above are the actual submission.
