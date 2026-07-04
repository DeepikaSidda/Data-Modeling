# Access Pattern Matrix — Virtual Waiting Room

Every access pattern the system performs, mapped to the **table or index**, the **key condition expression**, and the **filter expression** that serves it. **No pattern uses a `Scan`.**

Table: `WaitingRoom` (single table). Indexes: `WaitingIndex` (sparse GSI), `EligibilityIndex` (GSI). See the design document for full schema and rationale.

| # | Access Pattern | Table / Index | Key Condition Expression | Filter Expression | Operation | Requirement |
|---|---|---|---|---|---|---|
| 1 | Admit fan (exactly-once) | `WaitingRoom` (entry + guard) | Put entry `PK=EVT#e#SH#s, SK=ENTRY#ok`; put guard `PK=EVT#e#FAN#f, SK=ADMISSION` cond `attribute_not_exists(PK)` | none | `TransactWriteItems` (2 puts, conditional) | 1.1, 2.5, 9.1 |
| 1b | Increment sharded admit counter | `WaitingRoom` (admit counter) | `PK=EVT#e#SH#s AND SK=ADMIT_COUNT`, `ADD Admitted_Count :1` | none | `UpdateItem` (atomic ADD, same shard partition as entry) | 1.6 |
| 1c | Read total admitted (queue-full gate) | ElastiCache aggregate (Streams-fed `Σ ADMIT_COUNT`) | cached `total_admitted` vs `Max_Queue_Size` | none | cache read (exact fallback: `GetItem` per shard, summed) | 1.6 |
| 2 | Dedupe / lookup by Fan_Id | `WaitingRoom` (guard) | `PK=EVT#e#FAN#f AND SK=ADMISSION` | none | `GetItem` | 1.3 |
| 3 | Get status by token | `WaitingRoom` (entry) via DAX | `PK=EVT#e#SH#s AND SK=ENTRY#ok` (derived from token) | none | `GetItem` | 8.1 |
| 4 | Count WAITING ahead (exact / audit) | `WaitingIndex` per shard | `Waiting_Shard=EVT#e#SH#s AND Ordering_Key < :ok`, `Select=COUNT` | none (sparse index holds only WAITING) | `Query` × Shard_Count, summed | 8.3 |
| 4b | Approximate position (hot path) | ElastiCache aggregates | cached `admission_sequence_rank − Promoted_Total` | none | cache read | 8.3, 8.8 |
| 5 | Next WAITING in position order | `WaitingIndex` per shard | `Waiting_Shard=EVT#e#SH#s ORDER BY Ordering_Key ASC LIMIT n` | none (sparse index holds only WAITING) | `Query` × Shard_Count, k-way merge | 5.1, 5.6 |
| 6 | Query ELIGIBLE / ACTIVE for capacity & expiry | `EligibilityIndex` | `Elig_PK=EVT#e#ELIGIBLE AND Promotion_Time < :cutoff` | none (status encoded in PK) | `Query` | 2.4, 5.8, 6.3 |
| 6b | Read capacity counters | `WaitingRoom` (counter) | `PK=EVT#e AND SK=CAPACITY` | none | `GetItem` | 6.1–6.5 |
| 7 | Reserve downstream slots | `WaitingRoom` (counter) | `PK=EVT#e AND SK=CAPACITY` cond `Eligible_Count+Active_Count+:n <= Downstream_Capacity` | none | `UpdateItem` (atomic ADD, conditional) | 6.1, 6.4 |
| 8 | Promote entry (WAITING→ELIGIBLE) | `WaitingRoom` (entry) | `PK,SK` cond `Eligibility_Status = WAITING`, `REMOVE Waiting_Shard` | none | `UpdateItem` (conditional) | 5.2, 5.5 |
| 9 | Expire eligibility (ELIGIBLE→EXPIRED) | `WaitingRoom` (entry) | `PK,SK` cond `Eligibility_Status = ELIGIBLE` | none | `UpdateItem` (conditional) | 5.8, 10.1 |
| 10 | Activate / complete (ELIGIBLE→ACTIVE→COMPLETED) | `WaitingRoom` (entry) | `PK,SK` cond expected status | none | `UpdateItem` (conditional) | 10.1 |
| 11 | Read event config / open-state | `WaitingRoom` (config) | `PK=EVT#e AND SK=CONFIG` | none | `GetItem` | 1.5, 1.6 |
| 12 | Release freed slot | `WaitingRoom` (counter) | `PK=EVT#e AND SK=CAPACITY` ADD on decrement | none | `UpdateItem` (atomic ADD) | 6.3, 7.5 |

## Why every Filter Expression is `none`

A DynamoDB `FilterExpression` is applied **after** items are read from the table or index, so filtered-out items **still consume read capacity**. At 10M queue entries and millions of pollers, filtering would burn RCUs on rows that are immediately discarded.

This design pushes all selection into the key schema instead:

- **Status selection** is encoded into keys: the sparse `WaitingIndex` (its partition-key attribute `Waiting_Shard` exists only while an entry is `WAITING`) and the status-encoded `Elig_PK = EVT#<Event_Id>#<Eligibility_Status>` on `EligibilityIndex`.
- **Identity lookups** are served by token-derived primary keys (`GetItem`), never a query-plus-filter.

As a result the Filter Expression column reads `none` across the board, and every read pays only for the items it actually returns. This is a deliberate design outcome, not an omission.

## Index reference

| Index | Partition Key | Sort Key | Sparse? | Purpose |
|---|---|---|---|---|
| `WaitingRoom` (base) | `PK` | `SK` | no | All item types; token-derived point reads; sharded writes |
| `WaitingIndex` (GSI) | `Waiting_Shard` | `Ordering_Key` | **yes** (only while WAITING) | Read next WAITING in position order; per-shard position counts |
| `EligibilityIndex` (GSI) | `Elig_PK` | `Promotion_Time` | yes (ELIGIBLE/ACTIVE only) | Capacity accounting, expiry sweep, status-by-status queries |

No Local Secondary Indexes (LSIs) are used; the design document explains why (LSIs share the base partition key — the write shard — and cannot serve the cross-shard/global reads this system requires).
