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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;

/**
 * @author lazycece
 * @date 2026/11/11
 */
class InMemoryRetryTriggerTest {

    @Test
    void triggersAndSubmitsRetryToDispatchExecutor() {
        @SuppressWarnings("unchecked")
        ObjectProvider<MessageDispatcher> provider = mock(ObjectProvider.class);
        MessageDispatcher dispatcher = mock(MessageDispatcher.class);
        when(provider.getObject()).thenReturn(dispatcher);
        Executor executor = mock(Executor.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(executor).execute(any());
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        InMemoryRetryTrigger retryTrigger = new InMemoryRetryTrigger(provider, executor, scheduler);

        retryTrigger.trigger(42L, 1500L);

        ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
        verify(scheduler).schedule(task.capture(), org.mockito.ArgumentMatchers.eq(1500L),
                org.mockito.ArgumentMatchers.eq(TimeUnit.MILLISECONDS));
        task.getValue().run();
        verify(dispatcher).retry(42L);
    }

    @Test
    void rejectedRetryDoesNotPropagate() {
        @SuppressWarnings("unchecked")
        ObjectProvider<MessageDispatcher> provider = mock(ObjectProvider.class);
        when(provider.getObject()).thenReturn(mock(MessageDispatcher.class));
        Executor executor = mock(Executor.class);
        doThrow(new RejectedExecutionException("full")).when(executor).execute(any());
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        InMemoryRetryTrigger retryTrigger = new InMemoryRetryTrigger(provider, executor, scheduler);

        retryTrigger.trigger(42L, 0L);

        ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
        verify(scheduler).schedule(task.capture(), anyLong(), any(TimeUnit.class));
        assertThatCode(() -> task.getValue().run()).doesNotThrowAnyException();
    }
}
