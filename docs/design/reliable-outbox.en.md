**English** | [简体中文](reliable-outbox.md)

# reliable-outbox: A Reliable Messaging Component Based on the Transactional Outbox Pattern

This document describes the design trade-offs and external contracts of reliable-outbox. It focuses on the design rationale and the contracts that must be honored. For implementation details, see `reliable-outbox-core`; this document does not repeat the code.

## 1. Overview

### 1.1 Problem Statement

Writing business data and publishing a message are two separate operations. If the process crashes between them, or the message broker is briefly unavailable, the result is either a lost message or a message that has been sent while the corresponding data was never committed. reliable-outbox solves this with the Transactional Outbox (local message table) pattern: business data and the message record are written in the same local transaction. After the transaction commits, a background component performs delivery, and failures are retried.

This approach relies on neither distributed transactions nor the transactional message capability of an MQ. A single relational database plus any message broker is enough to achieve end-to-end eventual consistency.

### 1.2 Design Goals

- Business data and the message record are written atomically in one local transaction.
- The message broker is replaceable and not tied to a specific implementation.
- Scanning and cleaning have no built-in scheduler; they are driven by the integrator's scheduling framework.
- Sent messages are cleaned up automatically; dead letters are retained for manual handling.
- Two entry points, `sendInTransaction` and `sendStandalone`, with clear transaction semantics that share one internal flow.
- Safe under multi-instance concurrency; self-healing after a process crash.
- Dead letters support monitoring, alerting, and manual replay.

### 1.3 Capability Boundaries

- Delivery semantics are at-least-once. Duplicates are inevitable; consumers must be idempotent.
- Delivery order is not guaranteed, even within the same `partition_key`.
- The payload is capped at 64KB (65,535 bytes) and headers at 1KB (1,024 bytes) by default.
- Time uses the JVM local time throughout, with columns typed `DATETIME(3)` and no timezone conversion.

Full constraints are in Section 10.

## 2. Overall Design

Business data and the message record are written in the same transaction to guarantee atomicity; after commit, a separate component delivers to the MQ, retrying on failure. The component is integrated as a library into the business service and deployed together with the business process.

The architecture is divided into four layers:

- Integration layer: `MessageClient`. The business chooses its own transaction boundary and calls `sendInTransaction` or `sendStandalone`.
- Core layer: transaction awareness, outbox record management, dispatch scheduling, retry, cleaning, dead-letter governance, and metrics reporting.
- Storage layer: the outbox table, located in the same database instance as the business database.
- External dependency layer: message broker, scheduling component, monitoring platform. This entire layer is isolated via SPI.

Module responsibilities:

| Module | Responsibility |
| --- | --- |
| `MessageClient` | Public entry points `sendInTransaction` / `sendStandalone`, sharing validation, persistence, and post-commit triggering |
| `TransactionExecutor` | Executes writes with the specified propagation behavior and registers commit callbacks |
| `OutboxRepository` | Message table reads/writes, state transitions, row-level lease claiming, and the place where database dialect adaptation lives |
| `MessageDispatcher` | Claiming, delivery, persisting success/failure, and scheduling retries |
| `MessagePublisher` | Delivery to a concrete MQ (SPI) |
| `MessageScanner` | `scan()` scans due PENDING rows; the sole scheduling source for first delivery and retries; no built-in scheduler |
| `OutboxCleaner` | `clean()` / `clean(shardItem, shardTotal)` deletes expired SENT rows in batches; no built-in scheduler |
| `DeadLetterHandler` | Dead-letter logging, alerting, and monitoring events |
| `DeadLetterReplayer` | Dead-letter replay with status conditions |
| `MessageListener` | Events for delivery, failure, dead letter, and lease lost, used for metrics and alerting |
| `ReliableOutboxProperties` | Unified configuration model |

## 3. Data Model

### 3.1 Outbox Table

Default table name is `t_outbox`.

| Column | Type | Description |
| --- | --- | --- |
| `id` | BIGINT | Auto-increment primary key |
| `msg_id` | VARCHAR(64) | Globally unique message ID, the business idempotency key |
| `biz_code` | VARCHAR(64) | Business code, distinguishing source system/module |
| `topic` | VARCHAR(255) | Target topic or queue |
| `partition_key` | VARCHAR(255) | Optional, passed through to the broker for partitioning and consumer grouping; no ordering guarantee |
| `payload` | TEXT | Message body, capped at 65,535 bytes |
| `headers` | VARCHAR(1024) | Additional attributes (traceId, tenantId, etc.), capped at 1,024 bytes |
| `status` | TINYINT | 0 PENDING / 1 SENT / 2 DEAD |
| `retry_count` | INT | Number of retries performed, excluding the first delivery |
| `next_retry_time` | DATETIME(3) | Earliest time the message can be scanned; nullable |
| `owner_token` | VARCHAR(64) | Token of the instance currently holding the lease |
| `owner_expire_at` | DATETIME(3) | Lease expiration time |
| `created_at` | DATETIME(3) | Creation time |
| `updated_at` | DATETIME(3) | Update time |
| `sent_at` | DATETIME(3) | Time of successful send; used as the filter for cleaning |
| `last_error` | VARCHAR(1000) | Most recent failure reason |

DDL (MySQL 8). Replace `t_outbox` to use a custom table name:

```sql
CREATE TABLE `t_outbox` (
  `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT 'Primary key ID, auto-increment',
  `msg_id` VARCHAR(64) NOT NULL COMMENT 'Unique message ID, globally unique',
  `biz_code` VARCHAR(64) NOT NULL COMMENT 'Business code, distinguishing business party or send scenario',
  `topic` VARCHAR(255) NOT NULL COMMENT 'Message topic',
  `partition_key` VARCHAR(255) DEFAULT NULL COMMENT 'Partition key for broker partition selection and consumer grouping; no ordering guarantee',
  `payload` TEXT NOT NULL COMMENT 'Message body, default cap 65535 bytes',
  `headers` VARCHAR(1024) DEFAULT NULL COMMENT 'Passthrough metadata (e.g. traceId, tenantId, schemaVersion), default cap 1024 bytes',
  `status` TINYINT NOT NULL DEFAULT 0 COMMENT 'Message status: 0 pending/retry, 1 sent, 2 dead letter',
  `retry_count` INT NOT NULL DEFAULT 0 COMMENT 'Number of retries performed',
  `next_retry_time` DATETIME(3) DEFAULT NULL COMMENT 'Next retry time (JVM local time); the scanner uses it to decide whether a message is due',
  `owner_token` VARCHAR(64) DEFAULT NULL COMMENT 'Owner identifier of the current delivery lease, preventing concurrent duplicate delivery',
  `owner_expire_at` DATETIME(3) DEFAULT NULL COMMENT 'Delivery lease expiration (JVM local time); after expiry another instance may claim it',
  `sent_at` DATETIME(3) DEFAULT NULL COMMENT 'Successful send time (JVM local time); the cleaning job filters on this column',
  `last_error` VARCHAR(1000) DEFAULT NULL COMMENT 'Exception from the most recent failed delivery',
  `created_at` DATETIME(3) NOT NULL COMMENT 'Record creation time (JVM local time)',
  `updated_at` DATETIME(3) NOT NULL ON UPDATE CURRENT_TIMESTAMP(3) COMMENT 'Record update time (JVM local time); refreshed automatically when not explicitly set',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_msg_id` (`msg_id`),
  KEY `idx_status_next_retry` (`status`, `next_retry_time`),
  KEY `idx_status_sent` (`status`, `sent_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

Design notes:

- Only three states are persisted: PENDING, SENT, DEAD. SENDING is not a persisted state; "currently being delivered" is represented by `status = PENDING` together with an unexpired `owner_token`, which can be determined by querying the database. One fewer state means one fewer class of state inconsistency.
- `owner_token` and `owner_expire_at` together provide concurrency control and failure recovery; do not remove them.
- `next_retry_time` is the earliest time the scanner may pick up the message: for a PENDING first delivery it is `created_at + dispatch.initial-grace`; for retries it is computed by backoff; for SENT and DEAD it is `NULL`.
- DEAD messages stay in the original table; they are neither migrated nor cleaned automatically. After manual investigation they can be replayed directly, or corrected and then replayed. Consequently, a DEAD backlog needs attention because the main table keeps growing.
- `msg_id` is generated by the component as a 32-character hexadecimal UUID by default. When supplied by the business, it must be 1–64 ASCII letters, digits, underscores, or hyphens, and unique within the table. The column uses the default utf8mb4 collation; if the default collation is case-insensitive, the unique constraint and equality queries are likewise case-insensitive—specify `COLLATE utf8mb4_bin` explicitly when byte-exact matching is required. If multiple business modules share one table and their `msgId`s may collide, change the unique key to `UNIQUE (biz_code, msg_id)` and include `biz_code` in queries.
- Two indexes are kept: `idx_status_next_retry` serves due scanning, and `idx_status_sent` serves cleaning (`status = 1 AND sent_at < ?`). No ordering-related indexes are provided; the component offers no ordering capability, and extra sorting indexes only add write amplification.

### 3.2 Conditional Updates That Concurrency Correctness Depends On

The correctness of the design mainly depends on the following conditional updates, which are more worth reviewing than the concrete implementation:

```sql
-- Claim (used by the scanner): succeeds only when the lease is free or expired and the message is actually due,
-- guaranteeing that exactly one instance wins for a given message
UPDATE t_outbox
   SET owner_token = ?, owner_expire_at = ?, updated_at = ?
 WHERE id = ? AND status = 0
   AND next_retry_time IS NOT NULL AND next_retry_time <= ?
   AND (owner_token IS NULL OR owner_expire_at < ?);

-- Send success: must still hold the lease; 0 affected rows means the lease was lost and duplicate delivery is possible
UPDATE t_outbox
   SET status = 1, owner_token = NULL, owner_expire_at = NULL,
       sent_at = ?, retry_count = ?, next_retry_time = NULL, last_error = NULL, updated_at = ?
 WHERE id = ? AND owner_token = ? AND status = 0;

-- Send failure but retryable: back to PENDING, release the lease, write the next retry time
UPDATE t_outbox
   SET status = 0, owner_token = NULL, owner_expire_at = NULL,
       retry_count = ?, next_retry_time = ?, last_error = ?, updated_at = ?
 WHERE id = ? AND owner_token = ? AND status = 0;

-- Due scan: select only ids to avoid reading large columns; stable ordering by (next_retry_time, id)
SELECT id FROM t_outbox
 WHERE status = 0 AND next_retry_time IS NOT NULL AND next_retry_time <= ?
   AND (owner_token IS NULL OR owner_expire_at < ?)
 ORDER BY next_retry_time, id LIMIT ?;
```

The failure update that transitions to DEAD has the same shape, only setting `status` to 2 and `next_retry_time` to `NULL`.

Actively triggered delivery—the post-commit fast path and dead-letter replay—uses `tryAcquire` without the `next_retry_time` condition; the scanner uses `tryAcquireDue` with the due condition, avoiding claiming a message that was just postponed.

Cleaning first locates the oldest SENT row, then deletes by interval:

```sql
SELECT MIN(sent_at) FROM t_outbox WHERE status = 1;
SELECT id FROM t_outbox WHERE status = 1 AND sent_at >= ? AND sent_at < ? ORDER BY sent_at, id LIMIT ?;
DELETE FROM t_outbox WHERE id IN (...) AND status = 1;
```

DELETE always includes `status = 1`, so repeated or concurrent execution never deletes the wrong rows.

### 3.3 Custom Table Name

The table name is configured via `reliable-outbox.table-name`, defaulting to `t_outbox`. Constraints:

- It denotes a single physical table under the current data source. It is fixed at startup, must not change at runtime, and does not support per-message or per-tenant table selection.
- It must match `[A-Za-z_][A-Za-z0-9_]{0,63}`; lowercase snake_case is recommended, and database reserved words should be avoided.
- The table name cannot be bound as a `?`. The repository must validate it before concatenating it into SQL. Writes, queries, claiming, updates, scanning, replay, and cleaning all use the same configured table name; `t_outbox` must never be hard-coded.
- The component does not create tables or perform migrations. When using a custom table name, create the table in advance using the DDL above with the name substituted. Column types and lengths may be relaxed (e.g. `MEDIUMTEXT`), but the application-level caps for `payload` / `headers` are still controlled by `limits.*-bytes`.
- All instances on the same business database must be configured with the same table name. The default implementation only accepts an unqualified name without schema/catalog; multi-datasource scenarios must designate the data source via a custom `OutboxRepository` or `@Qualifier`.

## 4. State Machine

`status` is stored as an integer: 0 = PENDING, 1 = SENT, 2 = DEAD.

| State | Value | Meaning |
| --- | --- | --- |
| PENDING | 0 | Awaiting first delivery or retry; remains PENDING after a first failure, with the scan as the fallback |
| SENT | 1 | Delivered to the MQ, awaiting cleaning |
| DEAD | 2 | Retries exhausted or a non-retryable error occurred; requires manual handling |

There are four transitions:

- PENDING → SENT: delivery succeeded after an active trigger or a scan claim.
- PENDING → PENDING: failure but retryable; update `retry_count` and `next_retry_time`.
- PENDING → DEAD: non-retryable, or the retry limit was reached.
- DEAD → PENDING: manual replay.

There is no SENDING state, and a message never drifts automatically from PENDING into another state. After a failure the message stays PENDING, and the scanner delivers it again once `next_retry_time` is due; only success moves it to SENT. Whether something counts as a "retry" is determined by whether `last_error` is empty.

## 5. Core Flows

### 5.1 Two Entry Points

The component has one message model and one delivery flow. The two entry points differ only in their transaction precondition:

- `sendInTransaction`: requires that the calling thread already has a transaction for the corresponding data source, and joins it with `MANDATORY`. It fails immediately if no transaction exists, preventing business data and the message from being split into two transactions.
- `sendStandalone`: opens a separate short transaction with `REQUIRES_NEW`. An existing outer transaction is suspended first; even if the outer transaction later rolls back, this message is not undone. Callers must explicitly accept this semantic.

### 5.2 Unified Internal Flow

Both entry points end up in the same internal flow:

1. Validate parameters, generate the message ID, and prepare the initial state.
2. `TransactionExecutor` picks `MANDATORY` or `REQUIRES_NEW` depending on the entry point.
3. Insert a PENDING record inside the transaction, with `next_retry_time = now + dispatch.initial-grace`. If the `msg_id` already exists, throw `DuplicateMessageException`.
4. Only after a successful insert register the `afterCommit` callback on the current transaction; if the insert fails, do not register, and let the exception propagate.
5. After the transaction commits, the callback submits the delivery task to a bounded thread pool; the calling thread does not wait for the MQ.
6. If the transaction rolls back, the record rolls back with it, and the callback does not fire.
7. If delivery throws, the thread pool rejects the task, or the process crashes, the record remains PENDING and the scanner picks it up when due.

The two entry points never produce two sets of message types, state machines, or delivery implementations. `afterCommit` does not publish to the MQ directly, nor does it propagate exceptions to the already-committed caller.

### 5.3 Fast Path and Fallback

The main path is asynchronous delivery in `afterCommit`; normally the message is delivered within milliseconds. Scanning is the fallback, not the main path. In a healthy system the candidate set of each scan round should be nearly empty; if it keeps hitting rows, the fast path has degraded (persistent thread-pool rejection, callback not fired, repeated instance crashes), and the fast path should be investigated first rather than tuning scan parameters.

A new message's `next_retry_time` is `created_at + initial-grace` (10 seconds by default), which prevents the scanner from picking it up prematurely before the active delivery has run. The grace period and the scan interval are independent. Even if the grace period is shorter than the scan interval it does not matter: scanning and active delivery contend for the same lease, and the side that loses simply exits without duplicate delivery.

The scan selects ids with `status = 0 AND next_retry_time <= now AND lease free or expired`, and submits claimed rows to the thread pool. Multiple instances scanning concurrently is safe: per-row claiming guarantees exactly one instance wins, at the cost of only empty scans. The component has no built-in scheduling thread; when and how often `scan()` is invoked is decided by the integrator via `@Scheduled`, xxl-job, ElasticJob, Quartz, etc.

Fallback recovery latency depends on `initial-grace`, `lease-duration`, and the integrator's scan interval:

| Case | Recovery latency |
| --- | --- |
| Fast-path failure (rejection, callback not fired, crash after commit) | within `initial-grace` + one scan interval |
| Crash after claiming the lease (before `markSent`/`markFailed` is written) | within `lease-duration` + one scan interval |
| Retry after publish failure | backoff time + one scan interval |

With the defaults of 10s / 30s and a 5s scan, this is roughly within 15s, within 35s, and backoff plus 0–5s respectively. Of the three, only the scan interval is controlled by the integrator, and it should be set according to the recovery time the business can tolerate.

### 5.4 Retry

After a delivery failure the exception is classified first: retryable failures compute the next time by exponential backoff; non-retryable ones (`NonRetryablePublishException`) go straight to DEAD. Retryable failures stay PENDING with the lease cleared, the failure reason recorded, and `next_retry_time` set. The scanner picks the message up again when `next_retry_time <= now` and delivers it; if the retry limit is reached it transitions to DEAD and triggers an alert.

Note that retry execution precision equals the scan interval. `next_retry_time` means "earliest pick-up time", not an exact execution time. With the default backoff of 5/15/35/75/135 seconds and a 5-second scan, retries actually land around 5–10, 15–20 seconds, etc.; with the scan interval relaxed to 30 seconds, they align to scan ticks and the time to reach DEAD moves later accordingly. To stay close to the default retry window, the scan interval should not exceed `retry.initial-interval` (5 seconds by default).

### 5.5 Cleaning

Only SENT rows whose `sent_at` predates the retention period are cleaned. DEAD rows are never deleted automatically.

Cleaning runs in batches with a per-round batch limit, avoiding long transactions that lock the table and slow replication. After each batch deletion it re-queries until fewer than one batch remain or the limit is reached. As with scanning, no scheduler is built in; `clean()` is called periodically by the integrator.

For the sharded `clean(shardItem, shardTotal)`: it first uses `MIN(sent_at)` to locate the oldest row, obtains the range `[minSent, now - retention)`, then splits it into `shardTotal` contiguous equal parts, and each shard deletes only its own segment. Within one round, the `minSent` read by each shard is essentially the same, and together they cover the whole range exactly, with no duplicates or gaps.

A single instance is `shardTotal = 1`; `clean()` and `clean(0, 1)` are the same path.

If shards are invoked at staggered times, a later shard may read a `minSent` that has already advanced, leaving a small number of rows undeleted at the high end of that round. This does not affect the final result: the lowest shard always starts from the globally oldest row every round, guaranteeing continuous progress, and subsequent rounds converge with no permanent omission.

### 5.6 Dead-Letter Replay

After the fault is fixed, operators trigger replay through a permission-controlled admin entry point:

1. Conditionally update DEAD back to PENDING: `WHERE msg_id = ? AND status = 2`.
2. At the same time reset `retry_count` to 0, clear the lease and error information, and set `next_retry_time` to the current time.
3. Deliver immediately once; on failure, enter normal retry.

The component provides only this basic capability. Call permissions, reason recording, and auditing are the integrator's responsibility. Replay may also produce duplicates; consumers still must be idempotent.

## 6. Concurrency and Reliability

### 6.1 Leases

Concurrency control relies entirely on a row-level lease. Before delivery, a conditional update claims the row: it succeeds only when `owner_token` is empty or `owner_expire_at` has expired, and the winning instance is responsible for sending.

If the holder crashes, the lease expires and is reclaimed by another instance or the scanner. Ordinary publish failures do not go through this path: `markFailed` clears the lease and writes `next_retry_time` together, so the lock is released immediately and retries follow backoff. Only a process crash—or a thread stuck before `markSent` / `markFailed` is written—requires waiting for the lease to expire naturally. The recovery tempo of these two cases differs significantly and should be distinguished during troubleshooting.

The lease duration only determines the minimum interval between two publishes of the same message; it does not constrain the Publisher's send duration. If a publish exceeds the remaining lease, another instance may take over and resend after the lease expires. This is within the bounds allowed by at-least-once semantics, backed by consumer idempotency; there is no need to derive a send timeout from the lease.

Before a task taken from the thread pool actually publishes, it re-reads the message and confirms the lease is still valid (`owner_expire_at` later than the current time). If expired, it returns the lease, abandons this delivery, and lets the scanner re-claim the row. This check only prevents publishing under an expired lease; it is not a correctness precondition—even if the lease expires mid-publish, duplicate delivery is within the allowed bounds.

A successful claim does not change the state, avoiding a SENDING state and additional inconsistency.

The cleaning job does not hold leases. Shards are isolated by time windows, and delete statements carry `status = 1`, so even if clock skew causes overlap, they never delete twice or delete the wrong rows.

### 6.2 At-Least-Once and Sources of Duplication

If a publish succeeds but `markSent` is not persisted (process crash, lease lost, database error), the message is delivered again in a later retry. This is the normal cost of at-least-once semantics; exactly-once is not pursued, and consumer idempotency is a hard requirement.

When `markSent` returns false, a WARN must be logged and reported via `MessageListener.onLeaseLost`. If such duplicates are silently ignored, they will become unobservable long term.

The `MessagePublisher` contract is: a normal return means the broker has acknowledged (e.g. Kafka `acks=all`, Rabbit publisher confirm). "Sent but unacknowledged" is always treated as failure.

### 6.3 Clock

The whole chain uses JVM local time, with columns uniformly typed `DATETIME(3)`. Code uses `LocalDateTime` directly; sub-millisecond precision is rounded by the database. No UTC conversion is performed, and values are never mixed with epoch millis.

Multi-instance deployments require NTP. When skew exists, due checks fire earlier or later; `tryAcquireDue` only ensures a given message is not claimed twice concurrently, not an exact due time.

### 6.4 Ordering (Non-Goal)

The component explicitly does not guarantee delivery order; this is a deliberate capability boundary.

`afterCommit` active triggering, scan retries, lease reclamation, and manual replay are inherently multiple concurrent paths, so messages under the same `partition_key` can easily arrive at the broker out of order. Multiple instances, batched scanning, backoff retries, and crash recovery all amplify disorder: once a message enters retry, its successors may be delivered first.

`partition_key` is only passed through verbatim to the broker for partitioning and consumer grouping. The component does not serialize delivery for identical keys and maintains no watermarks of any kind.

If ordering is truly required, it must be guaranteed at all three layers:

1. Producer side: state changes for the same business entity are committed serially (distributed lock, single writer, or optimistic versioning), and a monotonically increasing `version` is carried in the payload/headers.
2. Transport layer: `MessagePublisher` maps `partition_key` to the broker partition key so that the same key lands on the same partition.
3. Consumer side: consume serially by `partition_key`, or use `version` for conditional updates with last-write-wins.

If "delivery order strictly equal to commit order" is required, that is beyond the capability of a local message table; use CDC (binlog/WAL subscription) or a native database transactional queue instead, and accept head-of-line blocking and the associated operational cost.

### 6.5 Nested Transactions

`sendInTransaction` only guarantees that the outbox record commits together with the current physical transaction in which it participates; it does not guarantee that this is the outermost transaction. If the business method itself opens a `REQUIRES_NEW` transaction, the outbox record joins the inner transaction, and the callback fires as soon as the inner transaction commits—a later outer rollback cannot undo the message.

`sendStandalone` always uses `REQUIRES_NEW`; when called from an outer transaction it suspends the outer transaction and commits independently, so it is unsuitable for scenarios that expect the message to roll back with the outer transaction.

With multiple data sources, note: `TransactionExecutor` must use the same transaction manager as `OutboxRepository`, one that can participate in the business transaction. The mere presence of some Spring transaction on the thread does not mean the precondition of `sendInTransaction` is satisfied.

### 6.6 Multi-Instance Scanning

No leader election is needed. Per-row claiming guarantees a message is not delivered twice.

Multiple instances scanning concurrently is safe; it only causes empty scans and update contention, a performance cost rather than a correctness problem. The scan query is deterministic (`ORDER BY next_retry_time, id LIMIT batch`); each instance reads the same batch of queue heads, and per-row `tryAcquireDue` lets exactly one instance succeed per message, with the rest performing one extra `UPDATE` that matches nothing.

Scheduling the scan on every instance is recommended. The extra empty-scan cost is very low, and in exchange the fallback keeps working as long as one instance is alive. Narrowing to a single scanning instance should be done only when truly necessary, together with leader election or health checks; otherwise a failure of that instance stops the fallback.

Staggered jitter only makes sense when multiple instances schedule concurrently; it mitigates instances deployed together with the same period staying phase-aligned over time. The approach is to randomize the first delay (e.g. a value in `[0, scan interval)`) and add ±10%–20% period jitter. If only one instance scans, no jitter is needed; a fixed period is more stable, and jitter instead lengthens the fallback tail latency.

Jitter only reduces empty scans and contention; it does not change correctness. Even if multiple instances scan the same row simultaneously, the lease's conditional update lets only one through.

### 6.7 Backpressure

The delivery thread pool is fixed-size with a bounded queue, and the rejection policy is `AbortPolicy`.

When a task is rejected, the message stays PENDING and the scan is the fallback. If the scanner's own submission is also rejected after claiming, it should release the lease immediately to avoid waiting an entire lease period for nothing.

The queue must never be unbounded; otherwise memory keeps piling up when the MQ slows down.

### 6.8 Message Size

The default payload cap is 65,535 bytes (`limits.payload-bytes`, aligned with MySQL `TEXT`), and the headers cap is 1,024 bytes (aligned with `VARCHAR(1024)`).

Serialization is the caller's responsibility. The `payload` string the component receives is the final form that is persisted and delivered, and `validate()` checks these same final bytes, so there is no "small before conversion, oversized after conversion" problem—the component performs no conversion in between.

When the cap is exceeded, `MessageTooLargeException` is thrown, carrying the actual byte count and the cap; nothing is written to the outbox and no retry occurs. It is a runtime exception; throwing it inside a business transaction triggers a rollback. Do not catch it within the transaction and commit the business data anyway, or you get a silent inconsistency where "the business succeeded but the message is missing".

Application-level caps must not exceed the column limits. To allow larger messages, change the columns to `MEDIUMTEXT`, relax the startup validation, and re-evaluate the broker's `max.message.bytes`, MySQL's `max_allowed_packet` (1MB or more recommended, covering payload + headers plus SQL overhead), and the consumer's deserialization limits.

Boundary tests should at least cover: payload exactly 65,535 passes, 65,536 is rejected; headers 1,024 passes, 1,025 is rejected; multi-byte UTF-8 is counted in bytes; no outbox row is added after an oversize rejection.

Larger messages should use the Claim Check pattern: store the message body in object storage and keep only a reference in the payload. This capability is not built in yet.

## 7. Retry Strategy

Exponential backoff with random jitter is used:

```
wait before the k-th retry = min(initial interval × 2^(k-1), max interval) + random jitter (k starts at 1)
```

The defaults are an initial interval of 5 seconds, a max interval of 60 seconds, at most 5 retries (excluding the first), and a jitter cap of 1 second.

The timeline without jitter is as follows (the first delivery is #0): 0s → #1 5s → #2 15s → #3 35s → #4 75s → #5 135s. If the 5th retry still fails, `retry_count` reaches 5 and the message enters DEAD, i.e. the dead letter becomes visible after about 135 seconds (a little over two minutes).

Jitter's job is to spread out due times. Messages tend to expire in clusters: messages created in the same transaction or the same second share a first due time; during a broker outage, a batch of in-flight messages fails at once; when the scan interval divides the backoff interval evenly, successive generations of retries lock-step onto the same timestamps. Jitter only desynchronizes; it is not a throttling mechanism. Real protection against retry storms relies on batched scanning, bounded queues, and per-row claiming.

The default of 1 second is determined by two constraints: on one hand, it does not exceed 25% of the initial interval (a quarter of 5 seconds), avoiding distortion of the backoff rhythm; on the other hand, it is not smaller than the second-level granularity of clustered messages, or bursts within the same scan window cannot be spread out. When jitter is 0, random number generation must be skipped, otherwise `nextLong(0, 0)` throws.

`maxCount = 5` counts retries only, excluding the first delivery, so there are at most 6 deliveries in total. The `retry_count` column uses the same convention: it is 0 on a first success or failure, avoiding the ambiguity of "configured for 5 but the database shows 6". Manual replay resets it to 0, effectively granting a fresh budget.

The reasoning behind the defaults: retries are only for absorbing transient blips (a brief network interruption, a single broker restart, a routing refresh—about 30–90 seconds), and dead letters are the entry point for human intervention. The two-minute-plus window is roughly on the same order as Kafka's default `delivery.timeout.ms` of 2 minutes. Longer outages are persistent problems; infrastructure monitoring has already alerted, and continued retrying provides no new decision information while only delaying dead-letter alerts. If the integration environment commonly sees longer failure windows, increase `retry.max-count` explicitly; `retry.max-interval` defaults to 60 seconds, which keeps the tail constrained to the minute level.

Non-retryable errors do not participate in backoff and go straight to DEAD.

## 8. Interfaces and Extension Points

### 8.1 Core Contracts

The following interfaces are the stable public surface. The JDBC repository, Dispatcher, Scanner, Cleaner, and the like are internal details; see `reliable-outbox-core`.

```java
public interface MessageClient {
    /** Joins the existing business transaction (MANDATORY). Throws DuplicateMessageException if msg_id already exists. */
    String sendInTransaction(OutboxMessage message);
    /** Uses an independent REQUIRES_NEW transaction. */
    String sendStandalone(OutboxMessage message);
}

public interface MessagePublisher {   // SPI, implemented by the integrator or an official adapter
    /** A normal return means the broker has acknowledged; throw PublishException for retryable, NonRetryablePublishException for non-retryable. */
    void publish(OutboxMessage message) throws PublishException;
}

public interface MessageListener {    // SPI for metrics and alerting; may be called concurrently by delivery threads, so implementations must be thread-safe
    default void onSent(OutboxMessage message) {}
    default void onFailed(OutboxMessage message, int retryCount, Throwable error) {}
    default void onDead(OutboxMessage message) {}
    default void onLeaseLost(OutboxMessage message, String reason) {}
}

public interface TransactionExecutor {
    <T> T executeInExistingTransaction(Supplier<T> action);  // MANDATORY
    <T> T executeInNewTransaction(Supplier<T> action);       // REQUIRES_NEW
    void afterCommit(Runnable action);                       // Must be called during one of the transactions above
}

public interface OutboxRepository {   // Storage abstraction; concurrency correctness and dialect adaptation live here
    void insert(OutboxMessage message);
    OutboxMessage findById(Long id);
    OutboxMessage findByMsgId(String msgId);
    boolean tryAcquire(Long id, String ownerToken, LocalDateTime ownerExpireAt, LocalDateTime now);
    boolean tryAcquireDue(Long id, String ownerToken, LocalDateTime ownerExpireAt, LocalDateTime dueAt);
    boolean releaseLease(Long id, String ownerToken, LocalDateTime now);
    boolean markSent(Long id, String ownerToken, LocalDateTime sentAt, int retryCount);
    boolean markFailed(Long id, String ownerToken, String error, LocalDateTime nextRetryTime,
                       int retryCount, boolean dead, LocalDateTime now);
    boolean resetForReplay(String msgId, LocalDateTime nextRetryTime, LocalDateTime now);
    LocalDateTime minSentAt();
    List<Long> scanSentBetween(LocalDateTime from, LocalDateTime to, int limit);
    List<Long> scanDispatchableIds(int limit, LocalDateTime now);
    int deleteSent(List<Long> ids);
}
```

Scheduling-related components likewise only provide capabilities, with no built-in scheduler:

```java
public class MessageScanner     { public void scan(); }                          // Called periodically by the integrator
public class OutboxCleaner      { public void clean(); public void clean(int shardItem, int shardTotal); }
public class DeadLetterReplayer { public boolean replay(String msgId); }         // Recommended behind a permission-controlled admin entry point
```

### 8.2 Configuration

The prefix is uniformly `reliable-outbox`, and each group is validated separately at startup with errors carrying the full kebab-case key:

| Property | Default | Description |
| --- | --- | --- |
| `table-name` | `t_outbox` | Outbox table name, see 3.3 |
| `dispatch.threads` | 8 | Default dispatch thread-pool size; effective only for the default executor |
| `dispatch.queue-capacity` | 1000 | Bounded queue capacity of the default thread pool; effective only for the default executor |
| `dispatch.lease-duration` | 30s | Lease duration, which also determines crash recovery latency |
| `dispatch.initial-grace` | 10s | Offset for when a new message first becomes scannable; applies only to the first delivery |
| `scan.batch-size` | 100 | Maximum rows returned by one scan query |
| `scan.max-batches-per-run` | 10 | Maximum batches processed per scan round |
| `retry.max-count` | 5 | Maximum retry count (excluding the first delivery) |
| `retry.initial-interval` | 5s | Backoff base for the first retry |
| `retry.max-interval` | 60s | Upper bound of the backoff interval |
| `retry.jitter` | 1s | Retry jitter cap |
| `clean.retention` | 7d | SENT retention period |
| `clean.batch-size` | 1000 | Rows per delete batch |
| `clean.max-batches-per-run` | 10 | Maximum batches processed per cleaning round |
| `limits.payload-bytes` | 65535 | Payload byte cap; must not exceed the column limit |
| `limits.headers-bytes` | 1024 | Headers byte cap; must not exceed the column limit |

Conventions:

- Startup validation covers several settings that can fail silently: thread count, queue capacity, batch sizes, and retry count must be ≥ 1; `retry.initial-interval ≤ retry.max-interval`; `retry.jitter ≥ 0`; `clean.retention > 0`; `limits.payload-bytes ≤ 65535` and `limits.headers-bytes ≤ 1024`. The last two align with the default DDL, preventing the application from admitting a message that fails on insert.
- `lease-duration` does not limit the Publisher's send duration and needs no margin; only lease validity is checked before delivery.
- `initial-grace` is merely a de-jitter window for the fast path, not a correctness parameter. Even if the scanner runs first, it will not duplicate delivery (the lease guarantees this). Its value can be estimated from fast-path queueing time: `queueCapacity / threads × publish duration P`, about 125× P with defaults.
- `scan` / `clean` provide only per-round batch parameters; the period is decided by the integrator and is not provided by the component.
- `threads` / `queue-capacity` apply only to the default `reliableOutboxDispatchExecutor`; once the integrator overrides that bean they no longer take effect.

### 8.3 Extension Points

| Extension point | Responsibility | Common implementations |
| --- | --- | --- |
| `OutboxRepository` | Table reads/writes and lease claiming; adapts dialect and table structure | MySQL / PostgreSQL / Oracle |
| `TransactionExecutor` | Executes MANDATORY / REQUIRES_NEW transactions and registers commit callbacks | Spring Transaction / other transaction frameworks |
| `MessagePublisher` | Delivers to a concrete MQ; a normal return means acknowledged | Kafka / RocketMQ / RabbitMQ / Pulsar |
| `MessageListener` | Success, failure, dead-letter, and lease-lost events for metrics and alerting | Micrometer / DingTalk / WeCom / email |

Official MQ adapters:

- `reliable-outbox-kafka`: reuses Spring Boot's `KafkaTemplate` (wired whenever present). The acknowledgement timeout is controlled by `reliable-outbox.kafka.send-timeout`, default 5 seconds.
- `reliable-outbox-rocketmq`: prefers a `DefaultMQProducer` bean; if absent, creates and starts one from `reliable-outbox.rocketmq.namesrv-addr` / `producer-group`, with `send-timeout` defaulting to 5 seconds. This adapter does not handle `partitionKey` (the component does not guarantee ordering).

Two caveats. Only one adapter should be enabled per application; when several coexist, disable the extra ones via `reliable-outbox.<broker>.enabled=false`, or let the business provide a routing `MessagePublisher`. If no adapter module is on the classpath, you must provide `MessagePublisher` yourself; otherwise startup fails with an explicit message naming the missing piece.

`MessagePublisher` should set its own send timeout to avoid threads or connections blocking for a long time. A publish exceeding the remaining lease adds at most one duplicate and does not affect at-least-once semantics.

The envelope (topic, partition key, headers, msgId) is provided uniformly by the component, and each adapter maps it to concrete MQ attributes. Serialization is on the caller side; the component delivers bytes verbatim and validates against the byte caps. If the broker has no stable partitioning semantics (e.g. RocketMQ queues are load-balanced at send time), the adapter may ignore `partition_key`.

With multiple data sources, explicitly designate the business database's `DataSource` (`@Qualifier` or a custom bean override).

### 8.4 Auto-Configuration

The component is wired via Spring Boot auto-configuration. All beans are annotated with `@ConditionalOnMissingBean`, so `MessagePublisher`, `OutboxRepository`, `MessageListener`, etc. can be overridden as needed.

When there is exactly one `PlatformTransactionManager`, a `SpringTransactionExecutor` is created automatically. When there are several, you must provide a `TransactionExecutor` matching the outbox data source yourself; otherwise `sendInTransaction` may bind to the wrong transaction.

Auto-configuration creates no scheduling thread pool and has no built-in scan/clean scheduling, so `@EnableScheduling` is not required. The only bean with threads is `reliableOutboxDispatchExecutor`, used for asynchronous delivery. Retries are likewise driven solely by the scanner.

## 9. Deployment and Operations

### 9.1 Deployment

Integrate as a library dependency into the business service and deploy with the process. The outbox table resides in the same database instance as the business database; this is the prerequisite for atomic local-transaction commit.

Schedule scanning and cleaning on each instance; per-row claiming guarantees no duplicate delivery, so no leader election is needed.

All instances must enable NTP. Columns are uniformly `DATETIME(3)`, and code binds `LocalDateTime` without timezone conversion.

`MessagePublisher`'s send timeout should be significantly shorter than `dispatch.lease-duration`, e.g. 5–10 seconds. Then, if a send hangs, it can throw before the lease expires, go through failure persistence, and release the lease immediately, shortening "stuck recovery" from waiting for lease expiry to seconds. The scan interval is also recommended not to exceed the lease duration; otherwise crash recovery is dominated by the scan interval.

When multiple instances schedule `scan()`, add a random first delay and period jitter (see 6.6); if only one instance scans, no jitter is needed. The scan interval also determines fallback recovery speed (see 5.3) and should be set according to the recovery time the business can tolerate.

### 9.2 Monitoring Metrics

Integrating Micrometer through `MessageListener` is recommended; focus on:

- Total outbox rows and counts per state.
- PENDING backlog, longest dwell time, and retry distribution by `retry_count`.
- DEAD count; this should be a key alert metric.
- Delivery success rate, average delivery latency, and publish timeout count.
- Scan duration, rows scanned per round, thread-pool rejection count, and `markSent` failure count (a risk signal for duplicate delivery).
- Cleaning job execution frequency and rows deleted.

### 9.3 Alerting

- Alert immediately whenever DEAD appears, including `msgId / bizCode / topic / retryCount / lastError`.
- PENDING backlog exceeds a threshold; delivery success rate falls below a threshold.
- `markSent` failures, persistent thread-pool rejections, long periods without a scan.
- Continuous outbox table growth; DEAD growth alerted separately.

Alerts need deduplication and rate limiting to avoid dead-letter storms.

### 9.4 Cleaning

SENT rows are retained for 7 days by default, then deleted in batches. DEAD rows are neither deleted nor migrated; after manual investigation they are replayed, or corrected and then replayed, and the message always stays in the original table.

## 10. Constraints and Evolution

### 10.1 Established Constraints

- Delivery semantics are at-least-once; consumer idempotency is a hard requirement. Retries, lease reclamation, and crash recovery all produce duplicates.
- No ordering guarantee, even within the same `partition_key`. When ordering is needed, serial commit on the producer side, broker partitioning by key, and serial consumption by key or conditional updates by version must all be implemented by the integrator.
- Large messages: payload ≤ 65,535 bytes and headers ≤ 1,024 bytes by default; exceeding them throws `MessageTooLargeException` without persisting. Claim Check is not built in yet.
- Transactions: `sendInTransaction` joins the current physical transaction with MANDATORY; `sendStandalone` commits independently with REQUIRES_NEW. Both are responsible only for the transaction boundary they actually participate in.
- Time: the database uniformly uses `DATETIME(3)` (local time, millisecond precision); code uses `LocalDateTime` directly with no timezone conversion.
- Multiple instances: correctness is guaranteed by lease claiming, with no leader election; scan and clean scheduling are left to the integrator.
- Publish contract: a normal return from `publish` means the broker has acknowledged. The send timeout is set by the Publisher itself, and the outbox does not constrain the value; a publish exceeding the lease only adds duplicates and does not affect at-least-once semantics.

### 10.2 Evolution Directions

- Add more MQ adapters and improve monitoring dashboards and operational tooling.
- Evaluate Claim Check for large messages and batched replay.
- If strict delivery ordering is truly required, evaluate CDC (binlog/WAL subscription) or a native database transactional queue first; do not use the local message table as an ordering channel.
- Whether to provide admin endpoints (dead-letter list, details, replay, metrics queries) and the accompanying permission model remains to be decided.
