/*
 *    Copyright (C) $YEAR lazycece<lazycece@gmail.com>. All rights reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.lazycece.outbox.core.config;

import com.lazycece.outbox.core.spi.MessagePublisher;
import jakarta.annotation.PostConstruct;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author lazycece
 */
@ConfigurationProperties(prefix = "reliable-outbox")
public class ReliableOutboxProperties {

    private boolean enabled = true;

    private String tableName = "t_outbox";

    private final Dispatch dispatch = new Dispatch();

    private final Scan scan = new Scan();

    private final Retry retry = new Retry();

    private final Clean clean = new Clean();

    private final Limits limits = new Limits();

    private final Admin admin = new Admin();

    @PostConstruct
    public void validate() {
        dispatch.validate();
        scan.validate();
        retry.validate();
        clean.validate();
        limits.validate();
        admin.validate();
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Dispatch getDispatch() {
        return dispatch;
    }

    public Scan getScan() {
        return scan;
    }

    public Retry getRetry() {
        return retry;
    }

    public Clean getClean() {
        return clean;
    }

    public Limits getLimits() {
        return limits;
    }

    public Admin getAdmin() {
        return admin;
    }

    /**
     * Post-commit dispatch and lease settings. {@code threads} and {@code queueCapacity} only apply
     * to the default {@code reliableOutboxDispatchExecutor} bean; they are ignored once the
     * integrator overrides that bean.
     */
    public static class Dispatch {

        /**
         * Number of threads in the default dispatch executor.
         */
        private int threads = 8;

        /**
         * Queue capacity of the default dispatch executor. When the queue is full, async dispatch
         * is abandoned and the scanner takes over.
         */
        private int queueCapacity = 1000;

        /**
         * Dispatch lease duration; only the instance holding the lease may publish.
         * <p>
         * The lease is re-validated right before publishing; if it has already expired, the message
         * releases the lease and is re-acquired by the scanner. A publish that takes longer than
         * the remaining lease only increases the chance of duplicate delivery and does not break
         * at-least-once semantics.
         * <p>
         * The lease also bounds recovery after the holder crashes: a crashed instance never writes
         * {@code markSent} / {@code markFailed}, so the message is re-acquired only after
         * {@code owner_expire_at} has passed and the next scan round runs. Recovery takes roughly
         * the remaining lease (at most this value) plus one scan period. The default 30 seconds
         * assumes publish latency in the milliseconds and crashes are not the normal path, trading
         * recovery latency against the duplicate window.
         * <p>
         * Configure the {@link MessagePublisher} send timeout well below
         * this value (e.g. 5 to 10 seconds): a stuck send then fails with an exception, is recorded
         * as a failure and releases the lease immediately instead of waiting for it to expire.
         */
        private Duration leaseDuration = Duration.ofSeconds(30);

        /**
         * Scan grace period for new messages: on insert,
         * {@code next_retry_time = created_at + initialGrace}, and the scanner picks the message up
         * only after that time. It applies to the first delivery only; once a delivery fails, the
         * next retry time comes from the backoff policy.
         * <p>
         * Fast-path dispatch (from {@code afterCommit}) uses {@code tryAcquire}, ignores
         * {@code next_retry_time} and publishes as soon as the transaction commits. The grace
         * period is therefore only a debounce window for the fast path: it keeps the scanner from
         * enqueueing the same row again while the fast-path task has not acquired the lease yet,
         * wasting queue capacity. It is not a correctness parameter: even if the scanner wins the
         * race, the lease guarantees a single publish, so missing a scan inside the window is
         * harmless.
         * <p>
         * If the fast path is lost (instance crash after commit, executor rejection,
         * {@code afterCommit} never fired), fallback discovery takes roughly the remaining grace
         * plus the next scan round, so this value should not be too large. Messages that crashed
         * after acquiring the lease are governed by lease expiry and are unaffected by this
         * setting.
         * <p>
         * Size it from the expected fast-path queueing time:
         * {@code queueCapacity / threads * publish latency P} (meaningful for the default executor
         * only), which is about {@code 125 * P} with the defaults. Healthy publish latency is
         * around 5 to 20ms, i.e. about 2.5s to drain the queue; the default 10s covers P up to
         * roughly 80ms with a slow broker. Values below 2s assume the fast path never queues and
         * are more likely to cause duplicate enqueues during slow-broker bursts.
         */
        private Duration initialGrace = Duration.ofSeconds(10);

        void validate() {
            if (threads < 1) {
                throw new IllegalStateException(
                        "reliable-outbox.dispatch.threads must be greater than 0");
            }
            if (queueCapacity < 1) {
                throw new IllegalStateException(
                        "reliable-outbox.dispatch.queue-capacity must be greater than 0");
            }
            if (leaseDuration.isZero() || leaseDuration.isNegative()) {
                throw new IllegalStateException(
                        "reliable-outbox.dispatch.lease-duration must be greater than 0");
            }
            if (initialGrace.isNegative()) {
                throw new IllegalStateException(
                        "reliable-outbox.dispatch.initial-grace must not be negative");
            }
        }

        public int getThreads() {
            return threads;
        }

        public void setThreads(int threads) {
            this.threads = threads;
        }

        public int getQueueCapacity() {
            return queueCapacity;
        }

        public void setQueueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
        }

        public Duration getLeaseDuration() {
            return leaseDuration;
        }

        public void setLeaseDuration(Duration leaseDuration) {
            this.leaseDuration = leaseDuration;
        }

        public Duration getInitialGrace() {
            return initialGrace;
        }

        public void setInitialGrace(Duration initialGrace) {
            this.initialGrace = initialGrace;
        }
    }

    /**
     * Per-run scanner settings.
     */
    public static class Scan {

        /**
         * Maximum number of message IDs returned by a single scan query.
         */
        private int batchSize = 100;

        /**
         * Maximum number of batches processed per scan run, bounding the resources one run
         * consumes.
         */
        private int maxBatchesPerRun = 10;

        void validate() {
            if (batchSize < 1) {
                throw new IllegalStateException(
                        "reliable-outbox.scan.batch-size must be greater than 0");
            }
            if (maxBatchesPerRun < 1) {
                throw new IllegalStateException(
                        "reliable-outbox.scan.max-batches-per-run must be greater than 0");
            }
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public int getMaxBatchesPerRun() {
            return maxBatchesPerRun;
        }

        public void setMaxBatchesPerRun(int maxBatchesPerRun) {
            this.maxBatchesPerRun = maxBatchesPerRun;
        }
    }

    /**
     * Failure retry and dead letter policy.
     */
    public static class Retry {

        /**
         * Maximum number of retries (excluding the first delivery); once reached, the message moves
         * to the dead letter (DEAD) status.
         * <p>
         * The default assumes retries only absorb transient hiccups and DEAD is the manual
         * intervention entry point: with a 5s initial and 60s capped backoff, the 5 retries happen
         * at about 5, 15, 35, 75 and 135 seconds (6 attempts including the first one), covering
         * common transient failures such as a single broker restart or routing refresh while
         * keeping DEAD alerts at the minute level. Integrators facing longer infrastructure failure
         * windows can raise it explicitly.
         */
        private int maxCount = 5;

        /**
         * Base backoff interval for the first retry; later retries grow exponentially.
         */
        private Duration initialInterval = Duration.ofSeconds(5);

        /**
         * Upper bound of the retry backoff interval.
         * <p>
         * Combined with the default {@code maxCount} it keeps the total retry window at the minute
         * level; when {@code maxCount} is raised, this cap still bounds the tail to one minute so
         * the backoff does not slide back into an hour-scale window.
         */
        private Duration maxInterval = Duration.ofSeconds(60);

        /**
         * Upper bound of the random jitter added to retry times, spreading messages that would
         * otherwise be retried at the same instant.
         * <p>
         * Jitter is added to every backoff interval and only de-synchronizes messages that failed
         * and become due together; it is not a rate limiter. Retry storms are guarded by batching,
         * the bounded executor queue and lease acquisition.
         * <p>
         * The default 1 second is the intersection of two constraints: no more than 25% of the
         * initial interval (5s), so exponential backoff is not distorted, and at least the
         * second-level granularity at which messages cluster, otherwise a burst within the same
         * scan window cannot be spread out. It adds at most 1s per retry and is negligible for the
         * total window; if the integrator's scan period is far above 1 second, de-synchronization
         * weakens, so it may be raised moderately but should stay well below the initial interval.
         */
        private Duration jitter = Duration.ofSeconds(1);

        void validate() {
            if (maxCount < 1) {
                throw new IllegalStateException(
                        "reliable-outbox.retry.max-count must be greater than 0");
            }
            if (initialInterval.isZero() || initialInterval.isNegative()) {
                throw new IllegalStateException(
                        "reliable-outbox.retry.initial-interval must be greater than 0");
            }
            if (maxInterval.compareTo(initialInterval) < 0) {
                throw new IllegalStateException(
                        "reliable-outbox.retry.max-interval must not be shorter than "
                                + "reliable-outbox.retry.initial-interval");
            }
            if (jitter.isNegative()) {
                throw new IllegalStateException(
                        "reliable-outbox.retry.jitter must not be negative");
            }
        }

        public int getMaxCount() {
            return maxCount;
        }

        public void setMaxCount(int maxCount) {
            this.maxCount = maxCount;
        }

        public Duration getInitialInterval() {
            return initialInterval;
        }

        public void setInitialInterval(Duration initialInterval) {
            this.initialInterval = initialInterval;
        }

        public Duration getMaxInterval() {
            return maxInterval;
        }

        public void setMaxInterval(Duration maxInterval) {
            this.maxInterval = maxInterval;
        }

        public Duration getJitter() {
            return jitter;
        }

        public void setJitter(Duration jitter) {
            this.jitter = jitter;
        }
    }

    /**
     * SENT message cleanup settings.
     */
    public static class Clean {

        /**
         * Retention period for SENT messages; messages sent earlier than this are deleted by the
         * cleanup task.
         */
        private Duration retention = Duration.ofDays(7);

        /**
         * Number of messages deleted per cleanup batch.
         */
        private int batchSize = 1000;

        /**
         * Maximum number of batches processed per cleanup run.
         */
        private int maxBatchesPerRun = 10;

        void validate() {
            if (retention.isZero() || retention.isNegative()) {
                throw new IllegalStateException(
                        "reliable-outbox.clean.retention must be greater than 0");
            }
            if (batchSize < 1) {
                throw new IllegalStateException(
                        "reliable-outbox.clean.batch-size must be greater than 0");
            }
            if (maxBatchesPerRun < 1) {
                throw new IllegalStateException(
                        "reliable-outbox.clean.max-batches-per-run must be greater than 0");
            }
        }

        public Duration getRetention() {
            return retention;
        }

        public void setRetention(Duration retention) {
            this.retention = retention;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public int getMaxBatchesPerRun() {
            return maxBatchesPerRun;
        }

        public void setMaxBatchesPerRun(int maxBatchesPerRun) {
            this.maxBatchesPerRun = maxBatchesPerRun;
        }
    }

    /**
     * Persisted field size limits, aligned with the column limits by default.
     */
    public static class Limits {

        /**
         * Maximum payload size in bytes; limited to 65,535 bytes by the TEXT column by default.
         */
        private int payloadBytes = 64 * 1024 - 1;

        /**
         * Maximum headers size in bytes; limited to 1,024 bytes by the VARCHAR(1024) column.
         */
        private int headersBytes = 1024;

        void validate() {
            if (payloadBytes <= 0 || payloadBytes > 65535) {
                throw new IllegalStateException(
                        "reliable-outbox.limits.payload-bytes must be between 1 and 65535 "
                                + "(TEXT limit); modify the payload column type and remove "
                                + "this check to allow larger messages");
            }
            if (headersBytes <= 0 || headersBytes > 1024) {
                throw new IllegalStateException(
                        "reliable-outbox.limits.headers-bytes must be between 1 and 1024 "
                                + "(VARCHAR(1024) limit)");
            }
        }

        public int getPayloadBytes() {
            return payloadBytes;
        }

        public void setPayloadBytes(int payloadBytes) {
            this.payloadBytes = payloadBytes;
        }

        public int getHeadersBytes() {
            return headersBytes;
        }

        public void setHeadersBytes(int headersBytes) {
            this.headersBytes = headersBytes;
        }
    }

    /**
     * Management operation settings.
     */
    public static class Admin {

        /**
         * Whether to expose the
         * {@link com.lazycece.outbox.core.admin.OutboxAdminService} bean. It is still only created
         * when the active {@code OutboxRepository} also implements
         * {@link com.lazycece.outbox.core.admin.OutboxAdminRepository}.
         */
        private boolean enabled = true;

        /**
         * Maximum number of {@code msg_id}s applied per SQL statement; larger batches are split
         * into several statements to bound statement size.
         */
        private int chunkSize = 20;

        void validate() {
            if (chunkSize < 1) {
                throw new IllegalStateException(
                        "reliable-outbox.admin.chunk-size must be greater than 0");
            }
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getChunkSize() {
            return chunkSize;
        }

        public void setChunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
        }
    }
}
