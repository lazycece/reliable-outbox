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
import java.util.Collection;

/**
 * Optional management operations over the outbox table, keyed by {@code msg_id}.
 *
 * <p>These are pure state transitions: no broker publish happens here, and the
 * message scanner remains the only dispatch source. A transitioned message is
 * picked up by the next scan round.
 *
 * <p>Kept separate from {@link com.lazycece.outbox.core.repository.OutboxRepository}
 * so custom repository implementations are not forced to implement management
 * operations. When the active repository does not implement this interface the
 * management service is not created.
 *
 * <p>Every method is a compare-and-set guarded by the expected status; operations on
 * {@code PENDING} rows additionally require the row not to be held under a live lease,
 * so management never races an in-flight dispatch. Methods return the number of rows
 * actually affected.
 *
 * @author lazycece
 */
public interface OutboxAdminRepository {

    /**
     * Resets {@code DEAD} messages back to {@code PENDING} with a fresh retry budget,
     * so the scanner re-dispatches them. Only {@code DEAD} messages are affected.
     *
     * @param msgIds business message ids
     * @param now    transition time, also used as {@code next_retry_time}
     * @return number of messages transitioned to {@code PENDING}
     */
    int replayDead(Collection<String> msgIds, LocalDateTime now);

    /**
     * Makes {@code PENDING} messages due immediately by setting {@code next_retry_time}
     * to {@code now}. Retry count and the last error are preserved. Messages held under
     * a live lease are skipped.
     *
     * @param msgIds business message ids
     * @param now    the new retry time and update time
     * @return number of messages made due
     */
    int retryPendingNow(Collection<String> msgIds, LocalDateTime now);

    /**
     * Forces {@code PENDING} messages to {@code DEAD}, recording {@code reason} in
     * {@code last_error}. The retry count is preserved. Messages held under a live
     * lease are skipped.
     *
     * @param msgIds business message ids
     * @param reason human-readable reason stored for later inspection
     * @param now    transition time
     * @return number of messages transitioned to {@code DEAD}
     */
    int markPendingAsDead(Collection<String> msgIds, String reason, LocalDateTime now);

    /**
     * Deletes {@code DEAD} messages. Only {@code DEAD} messages are affected.
     *
     * @param msgIds business message ids
     * @return number of messages deleted
     */
    int deleteDead(Collection<String> msgIds);
}
