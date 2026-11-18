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
package com.lazycece.outbox.core.repository;

import com.lazycece.outbox.core.model.OutboxMessage;
import java.time.LocalDateTime;
import java.util.List;

/**
 * @author lazycece
 * @date 2026/11/11
 */
public interface OutboxRepository {

    /**
     * Inserts the message and fills its generated id.
     *
     * @throws DuplicateMessageException when msg_id already exists
     */
    void insert(OutboxMessage message);

    OutboxMessage findById(Long id);

    OutboxMessage findByMsgId(String msgId);

    /**
     * Ignores next_retry_time; used for after-commit triggering and manual replay.
     */
    boolean tryAcquire(Long id, String ownerToken, LocalDateTime ownerExpireAt, LocalDateTime now);

    /**
     * Also requires next_retry_time <= dueAt; used by scanner and retry accelerator.
     */
    boolean tryAcquireDue(Long id, String ownerToken, LocalDateTime ownerExpireAt,
            LocalDateTime dueAt);

    boolean releaseLease(Long id, String ownerToken, LocalDateTime now);

    /**
     * @param retryCount retries performed before this successful attempt (0 when the first
     *                   delivery succeeds)
     */
    boolean markSent(Long id, String ownerToken, LocalDateTime sentAt, int retryCount);

    /**
     * dead=true means nextRetryTime must be null.
     */
    boolean markFailed(Long id, String ownerToken, String error, LocalDateTime nextRetryTime,
            int retryCount, boolean dead, LocalDateTime now);

    boolean resetForReplay(String msgId, LocalDateTime nextRetryTime, LocalDateTime now);

    /**
     * Returns ids only; ordered by next_retry_time, id ascending.
     */
    List<Long> scanDispatchableIds(int limit, LocalDateTime now);

    List<Long> scanSentBefore(LocalDateTime before, int limit);

    int deleteSent(List<Long> ids);
}
