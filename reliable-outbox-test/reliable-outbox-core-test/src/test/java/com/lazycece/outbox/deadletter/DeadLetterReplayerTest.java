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
package com.lazycece.outbox.deadletter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.lazycece.outbox.dispatch.MessageDispatcher;
import com.lazycece.outbox.model.OutboxMessage;
import com.lazycece.outbox.model.OutboxStatus;
import com.lazycece.outbox.support.InMemoryOutboxRepository;
import java.time.LocalDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author lazycece
 * @date 2026/11/11
 */
class DeadLetterReplayerTest {

    private InMemoryOutboxRepository repository;
    private MessageDispatcher dispatcher;
    private DeadLetterReplayer replayer;

    @BeforeEach
    void setUp() {
        repository = new InMemoryOutboxRepository();
        dispatcher = mock(MessageDispatcher.class);
        replayer = new DeadLetterReplayer(repository, dispatcher);
    }

    private OutboxMessage seed(String msgId, OutboxStatus status) {
        LocalDateTime now = LocalDateTime.now();
        OutboxMessage message = new OutboxMessage();
        message.setMsgId(msgId);
        message.setBizCode("order");
        message.setTopic("order-events");
        message.setPayload("{}");
        message.setStatus(status);
        message.setRetryCount(16);
        message.setNextRetryTime(now.plusSeconds(30));
        message.setCreatedAt(now);
        message.setUpdatedAt(now);
        return repository.put(message);
    }

    @Test
    void replayResetsDeadMessageAndTriggersDispatch() {
        OutboxMessage message = seed("m1", OutboxStatus.DEAD);

        assertThat(replayer.replay("m1")).isTrue();

        OutboxMessage reset = repository.get(message.getId());
        assertThat(reset.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(reset.getRetryCount()).isZero();
        assertThat(reset.getNextRetryTime()).isNotNull();
        verify(dispatcher).dispatchById(message.getId());
    }

    @Test
    void replayRejectsMissingOrNonDeadMessages() {
        seed("sent", OutboxStatus.SENT);

        assertThat(replayer.replay("missing")).isFalse();
        assertThat(replayer.replay("sent")).isFalse();
        verify(dispatcher, never()).dispatchById(org.mockito.ArgumentMatchers.anyLong());
    }
}
