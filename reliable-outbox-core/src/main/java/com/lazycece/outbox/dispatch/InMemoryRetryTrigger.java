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

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;

/**
 * Optional in-memory retry trigger. It is not registered by default; integrators who want retries
 * to run promptly at {@code next_retry_time} can register it themselves:
 *
 * <pre>{@code
 * @Bean
 * RetryTrigger retryTrigger(ObjectProvider<MessageDispatcher> dispatcherProvider,
 *         @Qualifier("reliableOutboxDispatchExecutor") ExecutorService executor,
 *         ScheduledExecutorService scheduler) {
 *     return new InMemoryRetryTrigger(dispatcherProvider, executor, scheduler);
 * }
 * }</pre>
 *
 * @author lazycece
 * @date 2026/11/11
 */
public class InMemoryRetryTrigger implements RetryTrigger {

    private static final Logger log = LoggerFactory.getLogger(InMemoryRetryTrigger.class);

    private final ObjectProvider<MessageDispatcher> dispatcherProvider;
    private final Executor dispatchExecutor;
    private final ScheduledExecutorService scheduler;

    public InMemoryRetryTrigger(ObjectProvider<MessageDispatcher> dispatcherProvider,
            Executor dispatchExecutor,
            ScheduledExecutorService scheduler) {
        this.dispatcherProvider = dispatcherProvider;
        this.dispatchExecutor = dispatchExecutor;
        this.scheduler = scheduler;
    }

    @Override
    public void trigger(Long messageId, long delayMillis) {
        scheduler.schedule(() -> {
            try {
                dispatchExecutor.execute(() -> dispatcherProvider.getObject().retry(messageId));
            } catch (RejectedExecutionException ex) {
                log.warn("retry dispatch rejected, id={} will be retried by scanner", messageId);
            }
        }, Math.max(0, delayMillis), TimeUnit.MILLISECONDS);
    }
}
