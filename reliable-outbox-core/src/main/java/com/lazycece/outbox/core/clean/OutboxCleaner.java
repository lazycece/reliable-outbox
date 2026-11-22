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
package com.lazycece.outbox.core.clean;

import com.lazycece.outbox.core.config.ReliableOutboxProperties;
import com.lazycece.outbox.core.repository.OutboxRepository;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deletes SENT messages whose retention period has expired.
 *
 * <p>This class provides the capability only and does not embed any scheduling. The
 * integrator should periodically invoke {@link #clean()} (single instance) or
 * {@link #clean(int, int)} (sharded) from its own scheduling framework (Spring
 * {@code @Scheduled}, xxl-job, ElasticJob, Quartz, etc.).
 *
 * @author lazycece
 * @date 2026/11/11
 */
public class OutboxCleaner {

    private static final Logger log = LoggerFactory.getLogger(OutboxCleaner.class);

    private final OutboxRepository repository;
    private final ReliableOutboxProperties properties;

    public OutboxCleaner(OutboxRepository repository, ReliableOutboxProperties properties) {
        this.repository = repository;
        this.properties = properties;
    }

    /**
     * Runs one clean round as a single instance: deletes every SENT message older than the
     * retention period. Equivalent to {@link #clean(int, int) clean(0, 1)}.
     */
    public void clean() {
        clean(0, 1);
    }

    /**
     * Runs one clean round for a single shard: deletes SENT messages older than the retention
     * period whose {@code sent_at} falls in this shard's contiguous slice of
     * {@code [minSent, now - retention)}. All shards together cover the range exactly once.
     *
     * @throws IllegalArgumentException when {@code shardTotal < 1} or {@code shardItem} is not
     *                                  in {@code [0, shardTotal)}
     */
    public void clean(int shardItem, int shardTotal) {
        if (shardTotal < 1) {
            throw new IllegalArgumentException("shardTotal must be greater than 0");
        }
        if (shardItem < 0 || shardItem >= shardTotal) {
            throw new IllegalArgumentException("shardItem must be in [0, shardTotal)");
        }

        LocalDateTime before = LocalDateTime.now().minus(properties.getClean().getRetention());
        LocalDateTime minSent = repository.minSentAt();
        if (minSent == null || !minSent.isBefore(before)) {
            return;
        }

        Window window = shardWindow(minSent, before, shardItem, shardTotal);
        if (!window.from().isBefore(window.to())) {
            return;
        }
        cleanWindow(window.from(), window.to());
    }

    /**
     * Splits {@code [minSent, before)} into {@code shardTotal} contiguous, half-open slices and
     * returns the slice owned by {@code shardItem}.
     */
    static Window shardWindow(LocalDateTime minSent, LocalDateTime before, int shardItem,
            int shardTotal) {
        long micros = ChronoUnit.MICROS.between(minSent, before);
        LocalDateTime from = minSent.plus(micros * shardItem / shardTotal, ChronoUnit.MICROS);
        LocalDateTime to = minSent.plus(micros * (shardItem + 1) / shardTotal, ChronoUnit.MICROS);
        return new Window(from, to);
    }

    private void cleanWindow(LocalDateTime from, LocalDateTime to) {
        int batchSize = properties.getClean().getBatchSize();
        int total = 0;
        for (int round = 0; round < properties.getClean().getMaxBatchesPerRun(); round++) {
            List<Long> ids = repository.scanSentBetween(from, to, batchSize);
            if (ids.isEmpty()) {
                break;
            }
            total += repository.deleteSent(ids);
            if (ids.size() < batchSize) {
                break;
            }
        }
        if (total > 0) {
            log.info("reliable-outbox cleaner deleted {} SENT messages", total);
        }
    }

    record Window(LocalDateTime from, LocalDateTime to) {
    }
}
