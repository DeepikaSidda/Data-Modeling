# Requirements Document

## Introduction

This document specifies the requirements for a DynamoDB-powered Virtual Waiting Room that fairly queues up to 10 million concurrent fans arriving within seconds for a high-demand ticket sale. The system assigns each fan a verifiable queue position, progressively promotes batches of fans from "waiting" to "eligible to browse" based on position and available downstream purchasing capacity, and provides fans with low-latency real-time status updates covering position, eligibility, and estimated wait time.

The design must absorb an extreme write burst without dropping or misordering fans, avoid DynamoDB hot partitions, prevent queue-position gaming, serve high-concurrency low-latency reads, and regulate the load released to downstream purchasing services so that they are never over-saturated.

The requirements below are organized by capability area. Each acceptance criterion follows a single EARS pattern and is written to be individually testable.

## Glossary

- **Waiting_Room**: The overall system that queues, positions, promotes, and reports status for fans during a ticket sale event.
- **Fan**: An end user attempting to enter the ticket sale. Identified by a unique, non-forgeable **Fan_Id**.
- **Fan_Id**: A server-issued unique identifier assigned to a fan upon entry. It is opaque to the fan and cannot be self-selected.
- **Event**: A single ticket sale instance with its own isolated queue. Identified by an **Event_Id**.
- **Queue_Entry**: The persisted record representing one fan's presence in the queue, containing Fan_Id, queue position ordering key, entry timestamp, eligibility status, and batch assignment.
- **Queue_Position**: A fan's ordinal rank within an event queue, derived from a total ordering of Queue_Entries. Position 1 is at the front.
- **Ordering_Key**: The deterministic sortable value used to totally order Queue_Entries. Composed of an entry sequence component and a randomized tie-breaker component.
- **Entry_Timestamp**: The server-assigned time at which a fan's Queue_Entry was admitted to the queue.
- **Tie_Breaker**: A server-generated random component appended to the Ordering_Key to resolve near-simultaneous arrivals fairly and to prevent gaming.
- **Eligibility_Status**: The lifecycle state of a Queue_Entry. One of: `WAITING`, `ELIGIBLE`, `ACTIVE`, `EXPIRED`, `COMPLETED`.
- **Batch**: A group of Queue_Entries promoted together from `WAITING` to `ELIGIBLE`. Identified by a **Batch_Id**.
- **Batch_Promoter**: The component that selects and promotes batches of fans based on position and available capacity.
- **Downstream_Capacity**: The maximum number of concurrently active purchasing fans the downstream purchasing service can serve, targeted at approximately 1,000.
- **Active_Fan**: A fan whose Eligibility_Status is `ACTIVE`, meaning they currently hold a purchasing slot.
- **Status_Reader**: The component that serves fan status queries (position, estimated wait time, eligibility).
- **Estimated_Wait_Time**: A computed projection of how long a fan will remain in `WAITING` before promotion, derived from position and observed promotion rate.
- **Entry_Token**: A signed, verifiable credential returned to a fan proving their Queue_Entry and Queue_Position.
- **Admission_Writer**: The component that admits arriving fans and writes their Queue_Entry.
- **Write_Shard**: A partition-key subdivision used to spread concurrent writes across many DynamoDB partitions to avoid hot partitions.

## Requirements

### Requirement 1: Fan Admission and Queue Entry Creation

**User Story:** As a fan arriving at a ticket sale, I want to be admitted to the waiting room and receive a queue entry, so that I have a guaranteed place in line.

#### Acceptance Criteria

1. WHEN a fan requests entry to an open Event, THE Admission_Writer SHALL create exactly one Queue_Entry for that fan and assign a unique Fan_Id.
2. WHEN a Queue_Entry is created, THE Admission_Writer SHALL record the Fan_Id, Event_Id, Entry_Timestamp, Ordering_Key, Eligibility_Status of `WAITING`, and a null Batch assignment.
3. IF a fan who already holds an active Queue_Entry for an Event requests entry again for the same Event, THEN THE Admission_Writer SHALL return the existing Queue_Entry rather than creating a duplicate.
4. WHEN a Queue_Entry is successfully created, THE Admission_Writer SHALL return an Entry_Token to the fan that encodes the Fan_Id, Event_Id, and Ordering_Key.
5. IF a fan requests entry to an Event that is not open, THEN THE Admission_Writer SHALL reject the request with a descriptive error code.
6. WHERE an Event enforces a maximum queue size, IF the queue has reached that maximum, THEN THE Admission_Writer SHALL reject further entry requests with a queue-full error code.

### Requirement 2: DynamoDB Table Design for High-Throughput Writes

**User Story:** As a system operator, I want the queue modeled in DynamoDB with a write-optimized key schema, so that 10 million arrivals within seconds are absorbed without hot partitions or throttling.

#### Acceptance Criteria

1. THE Waiting_Room SHALL persist each Queue_Entry in a DynamoDB table keyed by a partition key that combines Event_Id with a Write_Shard identifier and a sort key that encodes the Ordering_Key.
2. WHEN a Queue_Entry is written, THE Admission_Writer SHALL assign the Write_Shard by distributing entries across a configured shard count so that write volume is spread evenly across partitions.
3. THE Waiting_Room SHALL model each Queue_Entry so that its Fan_Id, Entry_Timestamp, Eligibility_Status, Batch_Id, and Ordering_Key are retrievable from a single item.
4. WHERE eligibility-based access is required, THE Waiting_Room SHALL maintain a secondary index that allows Queue_Entries to be queried by Event_Id and Eligibility_Status.
5. WHEN admitting fans during the initial burst, THE Admission_Writer SHALL use conditional writes that guarantee each Fan_Id maps to at most one Queue_Entry per Event.
6. IF a write is throttled by DynamoDB, THEN THE Admission_Writer SHALL retry the write using exponential backoff with jitter until it succeeds or a bounded retry limit is reached.
7. IF the bounded retry limit is reached without a successful write, THEN THE Admission_Writer SHALL return a retryable error code to the fan and SHALL NOT record a partial Queue_Entry.

### Requirement 3: Fair Queue Position Assignment

**User Story:** As a fan, I want my queue position assigned fairly based on when I arrived, so that no one can jump ahead of me.

#### Acceptance Criteria

1. WHEN a Queue_Entry is created, THE Admission_Writer SHALL compose its Ordering_Key from a monotonically non-decreasing sequence component derived from the Entry_Timestamp followed by a randomized Tie_Breaker component.
2. THE Waiting_Room SHALL define Queue_Position as the ordinal rank of a Queue_Entry when all Queue_Entries for an Event are sorted in ascending Ordering_Key order.
3. WHEN two or more fans are admitted within the same timestamp resolution window, THE Admission_Writer SHALL order those Queue_Entries by their randomized Tie_Breaker so that ordering does not depend on fan-controllable input.
4. THE Admission_Writer SHALL derive the Tie_Breaker from a server-side source of randomness that a fan cannot predict or influence.
5. THE Waiting_Room SHALL assign Entry_Timestamp values from a single authoritative server-side time source so that Queue_Entry ordering is independent of any client-supplied clock.
6. IF a server-side clock skew between admission nodes exceeds a configured bound, THEN THE Admission_Writer SHALL apply a monotonic sequence source that preserves admission order despite the skew.
7. FOR ALL pairs of Queue_Entries in an Event, the relative ordering produced by their Ordering_Keys SHALL be total and deterministic, so that repeated position computations for the same set of entries yield identical Queue_Positions.

### Requirement 4: Gaming and Position-Tampering Prevention

**User Story:** As an event organizer, I want the queue protected against manipulation, so that fans cannot cheat their way to a better position.

#### Acceptance Criteria

1. THE Admission_Writer SHALL assign Fan_Id, Ordering_Key, and Queue_Position using server-controlled values only, and SHALL ignore any client-supplied position, timestamp, or ordering value.
2. WHEN a fan presents an Entry_Token, THE Waiting_Room SHALL verify the token's integrity signature before honoring the Queue_Position it encodes.
3. IF an Entry_Token fails integrity verification, THEN THE Waiting_Room SHALL reject the request with an authentication error code and SHALL NOT alter the fan's Queue_Entry.
4. IF multiple entry requests are received for the same authenticated identity within a configured rate window, THEN THE Admission_Writer SHALL admit at most one Queue_Entry and reject the remainder with a rate-limit error code.
5. THE Tie_Breaker SHALL be generated such that a fan cannot increase the probability of receiving an earlier Queue_Position by submitting repeated or crafted requests.

### Requirement 5: Batch Promotion from Waiting to Eligible

**User Story:** As a fan waiting in line, I want to be promoted to browse when capacity frees up, so that I get my fair turn to purchase.

#### Acceptance Criteria

1. WHEN purchasing capacity becomes available, THE Batch_Promoter SHALL select the next Queue_Entries in ascending Queue_Position order whose Eligibility_Status is `WAITING`.
2. WHEN a Batch is selected, THE Batch_Promoter SHALL transition each selected Queue_Entry's Eligibility_Status from `WAITING` to `ELIGIBLE` and SHALL assign the Batch a Batch_Id.
3. THE Batch_Promoter SHALL determine Batch size as the number of currently available downstream purchasing slots, bounded by a configured maximum Batch size.
4. IF the number of available downstream slots is zero, THEN THE Batch_Promoter SHALL promote zero Queue_Entries during that promotion cycle.
5. WHEN promoting a Queue_Entry, THE Batch_Promoter SHALL use a conditional update that succeeds only if the entry's current Eligibility_Status is `WAITING`, so that no entry is promoted more than once.
6. THE Batch_Promoter SHALL promote fans strictly in Queue_Position order, so that a fan at an earlier position is never promoted after a fan at a later position within the same Event.
7. WHEN a Queue_Entry is promoted to `ELIGIBLE`, THE Batch_Promoter SHALL record the promotion time so that eligibility expiration can be enforced.
8. IF an `ELIGIBLE` fan does not begin purchasing within a configured eligibility window, THEN THE Batch_Promoter SHALL transition that Queue_Entry's Eligibility_Status to `EXPIRED` and SHALL release its reserved slot.

### Requirement 6: Downstream Over-Promotion Prevention

**User Story:** As an operator of the purchasing service, I want promotion capped to what my service can handle, so that it is never overwhelmed.

#### Acceptance Criteria

1. THE Batch_Promoter SHALL NOT promote more Queue_Entries than the currently available Downstream_Capacity in any promotion cycle.
2. WHILE the count of `ELIGIBLE` and `ACTIVE` fans equals the configured Downstream_Capacity, THE Batch_Promoter SHALL promote zero additional Queue_Entries.
3. WHEN counting available capacity, THE Batch_Promoter SHALL account for both `ELIGIBLE` fans awaiting activation and `ACTIVE` fans currently purchasing.
4. WHEN multiple promotion cycles run concurrently, THE Batch_Promoter SHALL coordinate capacity accounting so that the combined promotions do not exceed the available Downstream_Capacity.
5. WHEN a promotion cycle computes available capacity, THE Batch_Promoter SHALL compute remaining capacity as the configured Downstream_Capacity minus the combined count of `ELIGIBLE` and `ACTIVE` fans, and SHALL promote at most that remaining count so that a cycle proceeds only while remaining capacity is positive.

### Requirement 7: Active Purchasing Pool Regulation (Optional Stretch Goal)

**User Story:** As an event organizer, I want a steady pool of about 1,000 active purchasers maintained, so that throughput stays high without over-saturating checkout.

#### Acceptance Criteria

1. WHERE active-pool regulation is enabled, THE Batch_Promoter SHALL maintain the count of Active_Fans at approximately the configured target of 1,000.
2. WHERE active-pool regulation is enabled, WHEN the count of Active_Fans drops below the configured target, THE Batch_Promoter SHALL draw a new Batch from `WAITING` Queue_Entries sufficient to refill the pool toward the target.
3. WHERE active-pool regulation is enabled, THE Batch_Promoter SHALL refill the active pool incrementally as individual slots free up, and SHALL NOT wait for all active slots to empty before promoting the next Batch.
4. WHERE active-pool regulation is enabled, THE Batch_Promoter SHALL keep the count of Active_Fans within a configured tolerance band around the target so that the pool neither exceeds Downstream_Capacity nor idles below the target.
5. WHERE active-pool regulation is enabled, WHEN a fan's Eligibility_Status transitions to `COMPLETED` or `EXPIRED`, THE Batch_Promoter SHALL treat that fan's slot as freed for the next refill.
6. WHERE active-pool regulation is enabled, IF maintaining the tolerance band would cause the count of Active_Fans to exceed Downstream_Capacity, THEN THE Batch_Promoter SHALL respect the Downstream_Capacity limit even when doing so breaks the tolerance band.

### Requirement 8: Low-Latency Fan Status Queries

**User Story:** As a waiting fan, I want to check my current position, eligibility, and estimated wait time, so that I know where I stand in real time.

#### Acceptance Criteria

1. WHEN a fan queries status with a valid Entry_Token, THE Status_Reader SHALL return the fan's current Queue_Position, Eligibility_Status, and Estimated_Wait_Time.
2. WHEN serving a status query, THE Status_Reader SHALL read Queue_Entry data using indexed lookups that avoid full table or full queue scans.
3. THE Status_Reader SHALL compute Queue_Position as one plus the count of `WAITING` Queue_Entries with an Ordering_Key less than the querying fan's Ordering_Key, so that the fan at the front of the queue holds Queue_Position 1 and the count of fans ahead equals Queue_Position minus one.
4. WHEN computing Estimated_Wait_Time, THE Status_Reader SHALL derive the value from the fan's Queue_Position and the observed promotion rate.
5. IF a status query presents an invalid or unverifiable Entry_Token, THEN THE Status_Reader SHALL reject the query with an authentication error code.
6. WHEN a fan's Eligibility_Status is `ELIGIBLE` AND the fan's eligibility window has not expired AND downstream browsing is available, THE Status_Reader SHALL return an indication that the fan may proceed to browse.
7. IF a fan's Eligibility_Status is `ELIGIBLE` but the eligibility window has expired or downstream browsing is unavailable, THEN THE Status_Reader SHALL return an indication that the fan may not yet proceed to browse together with the reason.
8. WHERE status responses are cacheable, THE Status_Reader SHALL attach a caching directive bounding staleness to a configured maximum age so that repeated polling by millions of fans is absorbed without per-request queue recomputation.

### Requirement 9: Burst Handling, Ordering, and Fairness Guarantees

**User Story:** As an event organizer, I want the initial stampede handled without losing or reordering fans, so that the sale is fair and trusted.

#### Acceptance Criteria

1. WHEN up to 10,000,000 fans request entry to an Event within the initial burst window, THE Waiting_Room SHALL admit each successfully written fan exactly once with no duplicate Queue_Entries.
2. IF an admitted fan's Queue_Entry write is acknowledged, THEN THE Waiting_Room SHALL retain that Queue_Entry and SHALL NOT drop it during subsequent processing.
3. THE Waiting_Room SHALL preserve the relative Queue_Position ordering established at admission across all later promotion and status operations.
4. WHILE the initial burst is being absorbed, THE Admission_Writer SHALL distribute writes across Write_Shards so that no single DynamoDB partition receives a disproportionate share of write traffic.
5. WHEN the same set of Queue_Entries is evaluated more than once, THE Waiting_Room SHALL produce identical Queue_Position results, so that position computation is idempotent.

### Requirement 10: Eligibility State Lifecycle Integrity

**User Story:** As a system operator, I want fan eligibility states to follow a strict lifecycle, so that the queue never enters an inconsistent state.

#### Acceptance Criteria

1. THE Waiting_Room SHALL restrict Eligibility_Status transitions to the following: `WAITING` to `ELIGIBLE`, `ELIGIBLE` to `ACTIVE`, `ELIGIBLE` to `EXPIRED`, and `ACTIVE` to `COMPLETED`.
2. IF a state transition is requested that is not permitted by the defined lifecycle, THEN THE Waiting_Room SHALL reject the transition and SHALL leave the Queue_Entry unchanged.
3. WHEN transitioning a Queue_Entry's Eligibility_Status, THE Waiting_Room SHALL use a conditional write predicated on the expected current status so that concurrent transitions cannot corrupt the state.
4. THE Waiting_Room SHALL ensure that at any time a Queue_Entry holds exactly one Eligibility_Status value.
5. IF a conditional transition write fails because the expected current status did not match, THEN THE Waiting_Room SHALL roll back any partial side effects associated with the attempted transition and SHALL re-read the Queue_Entry's authoritative Eligibility_Status before retrying.
6. WHEN a transition completes, THE Waiting_Room SHALL validate that the persisted Eligibility_Status matches one of the permitted lifecycle values, and IF the persisted value is not a permitted value, THEN THE Waiting_Room SHALL flag the Queue_Entry for reconciliation and SHALL exclude it from promotion until reconciled.
