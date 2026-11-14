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
package com.lazycece.outbox.dispatch;

/**
 * SPI for triggering retries of failed messages earlier than the next scan round.
 *
 * @author lazycece
 * @date 2026/11/11
 */
public interface RetryTrigger {

    /**
     * Triggers a retry of a failed message earlier than the next scan round. The default
     * implementation ({@link NoopRetryTrigger}) does nothing, so retries are picked up by
     * the integrator-driven {@code MessageScanner} once {@code next_retry_time} is due.
     * Integrators can implement this SPI to trigger retries in their own way, such as
     * retrying immediately, scheduling a delayed retry, or publishing to a delay queue.
     *
     * <p>Note that {@code MessageDispatcher#retry} only acquires a lease once
     * {@code next_retry_time} is due, so triggering earlier than that has no effect
     * unless the implementation intentionally bypasses the backoff.
     *
     * @param messageId   the outbox message id
     * @param delayMillis delay in milliseconds from now before the retry should run
     */
    void trigger(Long messageId, long delayMillis);
}
