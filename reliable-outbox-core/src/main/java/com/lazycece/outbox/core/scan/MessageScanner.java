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
package com.lazycece.outbox.core.scan;

import com.lazycece.outbox.core.config.ReliableOutboxProperties;
import com.lazycece.outbox.core.dispatch.MessageDispatcher;
import com.lazycece.outbox.core.repository.OutboxRepository;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lazycece
 */
public class MessageScanner {

    private static final Logger log = LoggerFactory.getLogger(MessageScanner.class);

    private final OutboxRepository repository;
    private final MessageDispatcher dispatcher;
    private final ExecutorService dispatchExecutor;
    private final ReliableOutboxProperties properties;

    public MessageScanner(OutboxRepository repository,
            MessageDispatcher dispatcher,
            ExecutorService dispatchExecutor,
            ReliableOutboxProperties properties) {
        this.repository = repository;
        this.dispatcher = dispatcher;
        this.dispatchExecutor = dispatchExecutor;
        this.properties = properties;
    }

    /**
     * Runs one scan round: finds due messages, acquires their leases and submits them for
     * async dispatch. Invoked on demand by the integrator's scheduling framework.
     */
    public void scan() {
        int batchSize = properties.getScan().getBatchSize();
        for (int round = 0; round < properties.getScan().getMaxBatchesPerRun(); round++) {
            List<Long> ids = repository.scanDispatchableIds(batchSize, LocalDateTime.now());
            if (ids.isEmpty()) {
                return;
            }
            for (Long id : ids) {
                if (!hasExecutorCapacity()) {
                    return;
                }
                LocalDateTime now = LocalDateTime.now();
                String ownerToken = UUID.randomUUID().toString();
                if (!repository.tryAcquireDue(id, ownerToken,
                        now.plus(properties.getDispatch().getLeaseDuration()), now)) {
                    continue;
                }
                try {
                    dispatchExecutor.execute(() -> dispatcher.dispatchAcquired(id, ownerToken));
                } catch (RejectedExecutionException ex) {
                    log.warn("dispatch queue full, release lease, id={}", id);
                    repository.releaseLease(id, ownerToken, LocalDateTime.now());
                }
            }
            if (ids.size() < batchSize) {
                return;
            }
        }
    }

    private boolean hasExecutorCapacity() {
        return !(dispatchExecutor instanceof ThreadPoolExecutor executor)
                || executor.getQueue().remainingCapacity() > 0;
    }
}
