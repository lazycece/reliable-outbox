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
package com.lazycece.outbox.core.admin;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Management operations for the outbox, invoked by the integrator from its own
 * console, endpoints, or tooling. This class only orchestrates state transitions
 * over {@link OutboxAdminRepository}; it never publishes to a broker, so the
 * message scanner stays the single dispatch source.
 *
 * <p>Message ids are applied in chunks of {@code chunkSize} to bound each SQL
 * statement's size. The caller obtains {@code msg_id}s from its own queries.
 *
 * <p>Permissions, operator identity and audit are the integrator's responsibility.
 *
 * @author lazycece
 */
public class OutboxAdminService {

    private final OutboxAdminRepository repository;
    private final int chunkSize;

    public OutboxAdminService(OutboxAdminRepository repository, int chunkSize) {
        if (chunkSize < 1) {
            throw new IllegalArgumentException("chunkSize must be greater than 0");
        }
        this.repository = repository;
        this.chunkSize = chunkSize;
    }

    /**
     * Replays {@code DEAD} messages (see
     * {@link OutboxAdminRepository#replayDead(Collection, LocalDateTime)}).
     *
     * @return number of messages transitioned to {@code PENDING}
     */
    public int replayDead(Collection<String> msgIds) {
        LocalDateTime now = LocalDateTime.now();
        return forEachChunk(msgIds, chunk -> repository.replayDead(chunk, now));
    }

    /**
     * Makes {@code PENDING} messages due immediately (see
     * {@link OutboxAdminRepository#retryPendingNow(Collection, LocalDateTime)}).
     *
     * @return number of messages made due
     */
    public int retryPendingNow(Collection<String> msgIds) {
        LocalDateTime now = LocalDateTime.now();
        return forEachChunk(msgIds, chunk -> repository.retryPendingNow(chunk, now));
    }

    /**
     * Forces {@code PENDING} messages to {@code DEAD} (see
     * {@link OutboxAdminRepository#markPendingAsDead(Collection, String, LocalDateTime)}).
     *
     * @param reason human-readable reason stored in {@code last_error}; must not be blank
     * @return number of messages transitioned to {@code DEAD}
     * @throws IllegalArgumentException when {@code reason} is null or blank
     */
    public int markDead(Collection<String> msgIds, String reason) {
        if (reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("reason must not be blank");
        }
        LocalDateTime now = LocalDateTime.now();
        return forEachChunk(msgIds, chunk -> repository.markPendingAsDead(chunk, reason, now));
    }

    /**
     * Deletes {@code DEAD} messages (see
     * {@link OutboxAdminRepository#deleteDead(Collection)}).
     *
     * @return number of messages deleted
     */
    public int deleteDead(Collection<String> msgIds) {
        return forEachChunk(msgIds, repository::deleteDead);
    }

    private int forEachChunk(Collection<String> msgIds, ChunkOperation operation) {
        if (msgIds == null || msgIds.isEmpty()) {
            return 0;
        }
        List<String> ids = new ArrayList<>(msgIds);
        int total = 0;
        for (int from = 0; from < ids.size(); from += chunkSize) {
            int to = Math.min(from + chunkSize, ids.size());
            total += operation.apply(ids.subList(from, to));
        }
        return total;
    }

    @FunctionalInterface
    private interface ChunkOperation {
        int apply(List<String> chunk);
    }
}
