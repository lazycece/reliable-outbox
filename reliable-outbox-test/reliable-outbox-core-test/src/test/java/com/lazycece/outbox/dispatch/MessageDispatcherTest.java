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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.lazycece.outbox.deadletter.DeadLetterHandler;
import com.lazycece.outbox.model.OutboxMessage;
import com.lazycece.outbox.model.OutboxStatus;
import com.lazycece.outbox.spi.MessageListener;
import com.lazycece.outbox.spi.MessagePublisher;
import com.lazycece.outbox.spi.NonRetryablePublishException;
import com.lazycece.outbox.spi.PublishException;
import com.lazycece.outbox.support.InMemoryOutboxRepository;
import java.time.Duration;
import java.time.LocalDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author lazycece
 * @date 2026/11/11
 */
class MessageDispatcherTest {

    private InMemoryOutboxRepository repository;
    private MessagePublisher publisher;
    private MessageListener listener;
    private DeadLetterHandler deadLetterHandler;
    private MessageDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        repository = new InMemoryOutboxRepository();
        publisher = mock(MessagePublisher.class);
        listener = mock(MessageListener.class);
        deadLetterHandler = mock(DeadLetterHandler.class);
        RetryPolicy retryPolicy = new RetryPolicy(16, Duration.ofSeconds(5),
                Duration.ofSeconds(300), Duration.ZERO);
        dispatcher = new MessageDispatcher(repository, publisher, retryPolicy,
                deadLetterHandler, listener, Duration.ofSeconds(60));
    }

    private OutboxMessage seed(String msgId, OutboxStatus status, int retryCount) {
        LocalDateTime now = LocalDateTime.now();
        OutboxMessage message = new OutboxMessage();
        message.setMsgId(msgId);
        message.setBizCode("order");
        message.setTopic("order-events");
        message.setPayload("{}");
        message.setStatus(status);
        message.setRetryCount(retryCount);
        message.setNextRetryTime(status == OutboxStatus.FAILED
                ? now.minusSeconds(1) : now.plusSeconds(30));
        message.setCreatedAt(now);
        message.setUpdatedAt(now);
        return repository.put(message);
    }

    @Test
    void successfulPublishMarksSentAndNotifies() {
        OutboxMessage message = seed("m1", OutboxStatus.INIT, 0);

        dispatcher.dispatchById(message.getId());

        OutboxMessage stored = repository.get(message.getId());
        assertThat(stored.getStatus()).isEqualTo(OutboxStatus.SENT);
        assertThat(stored.getOwnerToken()).isNull();
        verify(publisher).publish(any(OutboxMessage.class));
        verify(listener).onSent(any(OutboxMessage.class));
    }

    @Test
    void retryableFailureSchedulesRetry() {
        OutboxMessage message = seed("m1", OutboxStatus.INIT, 0);
        doThrow(new PublishException("boom", null)).when(publisher).publish(any());

        dispatcher.dispatchById(message.getId());

        OutboxMessage stored = repository.get(message.getId());
        assertThat(stored.getStatus()).isEqualTo(OutboxStatus.FAILED);
        // The first delivery is not a retry: retry_count stays 0 and only the 1st retry is scheduled.
        assertThat(stored.getRetryCount()).isZero();
        assertThat(stored.getNextRetryTime()).isAfter(LocalDateTime.now());
        assertThat(stored.getOwnerToken()).isNull();
        assertThat(stored.getLastError()).isEqualTo("boom");
        verify(listener).onFailed(any(OutboxMessage.class), eq(0), any(PublishException.class));
    }

    @Test
    void retryFailureIncrementsRetryCount() {
        OutboxMessage message = seed("m1", OutboxStatus.FAILED, 0);
        doThrow(new PublishException("boom", null)).when(publisher).publish(any());

        dispatcher.dispatchById(message.getId());

        OutboxMessage stored = repository.get(message.getId());
        assertThat(stored.getStatus()).isEqualTo(OutboxStatus.FAILED);
        assertThat(stored.getRetryCount()).isEqualTo(1);
        assertThat(stored.getNextRetryTime()).isAfter(LocalDateTime.now());
        verify(listener).onFailed(any(OutboxMessage.class), eq(1), any(PublishException.class));
    }

    @Test
    void successfulRetryPersistsRetryCount() {
        OutboxMessage message = seed("m1", OutboxStatus.FAILED, 1);

        dispatcher.dispatchById(message.getId());

        OutboxMessage stored = repository.get(message.getId());
        assertThat(stored.getStatus()).isEqualTo(OutboxStatus.SENT);
        assertThat(stored.getRetryCount()).isEqualTo(2);
        verify(listener).onSent(any(OutboxMessage.class));
    }

    @Test
    void nonRetryableFailureMarksDead() {
        OutboxMessage message = seed("m1", OutboxStatus.INIT, 0);
        doThrow(new NonRetryablePublishException("bad topic", null)).when(publisher).publish(any());

        dispatcher.dispatchById(message.getId());

        OutboxMessage stored = repository.get(message.getId());
        assertThat(stored.getStatus()).isEqualTo(OutboxStatus.DEAD);
        assertThat(stored.getNextRetryTime()).isNull();
        assertThat(stored.getRetryCount()).isZero();
        verify(deadLetterHandler).onDead(any(OutboxMessage.class));
    }

    @Test
    void failureAtMaxRetryCountMarksDead() {
        OutboxMessage message = seed("m1", OutboxStatus.FAILED, 15);
        doThrow(new PublishException("boom", null)).when(publisher).publish(any());

        dispatcher.dispatchById(message.getId());

        OutboxMessage stored = repository.get(message.getId());
        assertThat(stored.getStatus()).isEqualTo(OutboxStatus.DEAD);
        assertThat(stored.getRetryCount()).isEqualTo(16);
        verify(deadLetterHandler).onDead(any(OutboxMessage.class));
    }

    @Test
    void expiredLeaseIsReleasedWithoutPublishing() {
        OutboxMessage message = seed("m1", OutboxStatus.INIT, 0);
        LocalDateTime now = LocalDateTime.now();
        repository.tryAcquire(message.getId(), "t", now.minusSeconds(1), now);

        dispatcher.dispatchAcquired(message.getId(), "t");

        verify(publisher, never()).publish(any());
        assertThat(repository.get(message.getId()).getOwnerToken()).isNull();
    }

    @Test
    void stillValidLeaseIsPublishedEvenWithLittleTimeLeft() {
        OutboxMessage message = seed("m1", OutboxStatus.INIT, 0);
        LocalDateTime now = LocalDateTime.now();
        repository.tryAcquire(message.getId(), "t", now.plusSeconds(1), now);

        dispatcher.dispatchAcquired(message.getId(), "t");

        verify(publisher).publish(any(OutboxMessage.class));
        assertThat(repository.get(message.getId()).getStatus()).isEqualTo(OutboxStatus.SENT);
    }

    @Test
    void staleOwnerIsSkippedAndReported() {
        OutboxMessage message = seed("m1", OutboxStatus.INIT, 0);
        LocalDateTime now = LocalDateTime.now();
        repository.tryAcquire(message.getId(), "owner", now.plusSeconds(60), now);

        dispatcher.dispatchAcquired(message.getId(), "other");

        verify(publisher, never()).publish(any());
        verify(listener).onLeaseLost(any(OutboxMessage.class), eq("lease-lost-before-publish"));
    }

    @Test
    void markSentFailureReportsLeaseLoss() {
        InMemoryOutboxRepository spyRepository = spy(repository);
        OutboxMessage message = spyRepository.put(seedMessage("m1"));
        doReturn(false).when(spyRepository).markSent(anyLong(), any(), any(), anyInt());
        MessageDispatcher spyDispatcher = new MessageDispatcher(spyRepository, publisher,
                new RetryPolicy(16, Duration.ofSeconds(5), Duration.ofSeconds(300), Duration.ZERO),
                deadLetterHandler, listener, Duration.ofSeconds(60));

        spyDispatcher.dispatchById(message.getId());

        verify(publisher).publish(any(OutboxMessage.class));
        verify(listener).onLeaseLost(any(OutboxMessage.class), eq("mark-sent-failed"));
    }

    private OutboxMessage seedMessage(String msgId) {
        LocalDateTime now = LocalDateTime.now();
        OutboxMessage message = new OutboxMessage();
        message.setMsgId(msgId);
        message.setBizCode("order");
        message.setTopic("order-events");
        message.setPayload("{}");
        message.setStatus(OutboxStatus.INIT);
        message.setNextRetryTime(now.plusSeconds(30));
        message.setCreatedAt(now);
        message.setUpdatedAt(now);
        return message;
    }
}
