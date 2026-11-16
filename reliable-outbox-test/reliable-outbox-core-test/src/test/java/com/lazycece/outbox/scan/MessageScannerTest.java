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
package com.lazycece.outbox.scan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.lazycece.outbox.config.ReliableOutboxProperties;
import com.lazycece.outbox.dispatch.MessageDispatcher;
import com.lazycece.outbox.model.OutboxMessage;
import com.lazycece.outbox.model.OutboxStatus;
import com.lazycece.outbox.support.InMemoryOutboxRepository;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author lazycece
 * @date 2026/11/11
 */
class MessageScannerTest {

    private InMemoryOutboxRepository repository;
    private MessageDispatcher dispatcher;
    private ExecutorService executor;
    private ReliableOutboxProperties properties;
    private MessageScanner scanner;

    @BeforeEach
    void setUp() {
        repository = new InMemoryOutboxRepository();
        dispatcher = mock(MessageDispatcher.class);
        executor = mock(ExecutorService.class);
        properties = new ReliableOutboxProperties();
        properties.getScan().setBatchSize(2);
        properties.getScan().setMaxBatchesPerRun(10);
        scanner = new MessageScanner(repository, dispatcher, executor, properties);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(executor).execute(any());
    }

    private OutboxMessage seed(String msgId, LocalDateTime nextRetry) {
        LocalDateTime now = LocalDateTime.now();
        OutboxMessage message = new OutboxMessage();
        message.setMsgId(msgId);
        message.setBizCode("order");
        message.setTopic("order-events");
        message.setPayload("{}");
        message.setStatus(OutboxStatus.PENDING);
        message.setNextRetryTime(nextRetry);
        message.setCreatedAt(now);
        message.setUpdatedAt(now);
        return repository.put(message);
    }

    @Test
    void scansAllDueMessagesAcrossBatchesAndSkipsFutureOnes() {
        seed("due1", LocalDateTime.now().minusSeconds(10));
        seed("due2", LocalDateTime.now().minusSeconds(5));
        seed("due3", LocalDateTime.now().minusSeconds(1));
        seed("future", LocalDateTime.now().plusSeconds(60));

        scanner.scan();

        verify(dispatcher, org.mockito.Mockito.times(3))
                .dispatchAcquired(anyLong(), anyString());
    }

    @Test
    void rejectionReleasesTheLease() {
        OutboxMessage message = seed("due1", LocalDateTime.now().minusSeconds(1));
        doThrow(new RejectedExecutionException("full")).when(executor).execute(any());

        scanner.scan();

        assertThat(repository.get(message.getId()).getOwnerToken()).isNull();
        verify(dispatcher, never()).dispatchAcquired(anyLong(), anyString());
    }

    @Test
    void stopsWhenNothingIsDue() {
        scanner.scan();
        verify(dispatcher, never()).dispatchAcquired(anyLong(), anyString());
    }
}
