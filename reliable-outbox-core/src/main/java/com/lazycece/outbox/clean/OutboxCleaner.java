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
package com.lazycece.outbox.clean;

import com.lazycece.outbox.config.ReliableOutboxProperties;
import com.lazycece.outbox.repository.OutboxRepository;
import java.time.LocalDateTime;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deletes SENT messages whose retention period has expired.
 *
 * <p>This class provides the capability only and does not embed any scheduling. The
 * integrator should periodically invoke {@link #clean()} from its own scheduling framework (Spring
 * {@code @Scheduled}, xxl-job, ElasticJob, Quartz, etc.), for example:
 * <pre>{@code
 * @Scheduled(fixedDelayString = "${reliable-outbox.clean-interval:3600000}")
 * public void cleanOutbox() {
 *     outboxCleaner.clean();
 * }
 * }</pre>
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
     * Runs one clean round: deletes SENT messages older than the retention period in batches.
     * Invoked on demand by the integrator's scheduling framework.
     */
    public void clean() {
        LocalDateTime before = LocalDateTime.now().minus(properties.getSentRetention());
        int batchSize = properties.getCleanBatchSize();
        int total = 0;
        for (int round = 0; round < properties.getCleanMaxBatchesPerRun(); round++) {
            List<Long> ids = repository.scanSentBefore(before, batchSize);
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
}
