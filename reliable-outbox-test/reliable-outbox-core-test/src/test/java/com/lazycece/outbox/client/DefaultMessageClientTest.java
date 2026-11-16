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
package com.lazycece.outbox.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.lazycece.outbox.config.ReliableOutboxProperties;
import com.lazycece.outbox.dispatch.MessageDispatcher;
import com.lazycece.outbox.model.OutboxMessage;
import com.lazycece.outbox.model.OutboxStatus;
import com.lazycece.outbox.repository.DuplicateMessageException;
import com.lazycece.outbox.repository.OutboxRepository;
import com.lazycece.outbox.transaction.TransactionExecutor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author lazycece
 * @date 2026/11/11
 */
class DefaultMessageClientTest {

    private OutboxRepository repository;
    private MessageDispatcher dispatcher;
    private ExecutorService dispatchExecutor;
    private ImmediateTransactionExecutor transactionExecutor;
    private ReliableOutboxProperties properties;
    private DefaultMessageClient client;

    @BeforeEach
    void setUp() {
        repository = mock(OutboxRepository.class);
        dispatcher = mock(MessageDispatcher.class);
        dispatchExecutor = mock(ExecutorService.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(dispatchExecutor).execute(any());
        transactionExecutor = new ImmediateTransactionExecutor();
        properties = new ReliableOutboxProperties();
        client = new DefaultMessageClient(repository, dispatcher, transactionExecutor,
                dispatchExecutor, properties);
        doAnswer(invocation -> {
            ((OutboxMessage) invocation.getArgument(0)).setId(7L);
            return null;
        }).when(repository).insert(any());
    }

    @Test
    void sendInTransactionPreparesPersistsAndDispatchesAfterCommit() {
        OutboxMessage message = message();

        String msgId = client.sendInTransaction(message);

        assertThat(msgId).isEqualTo(message.getMsgId()).hasSize(32);
        assertThat(transactionExecutor.usedExistingTransaction).isTrue();
        assertThat(message.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(message.getRetryCount()).isZero();
        assertThat(message.getCreatedAt()).isNotNull();
        assertThat(message.getNextRetryTime())
                .isEqualTo(message.getCreatedAt().plus(properties.getDispatch().getInitialGrace()));
        verify(repository).insert(message);
        verify(dispatcher).dispatchById(7L);
    }

    @Test
    void sendStandaloneUsesNewTransaction() {
        client.sendStandalone(message());

        assertThat(transactionExecutor.usedExistingTransaction).isFalse();
        verify(dispatcher).dispatchById(7L);
    }

    @Test
    void duplicateMsgIdPropagatesExceptionWithoutDispatch() {
        doThrow(new DuplicateMessageException("dup")).when(repository).insert(any());
        OutboxMessage message = message();
        message.setMsgId("dup");

        assertThatThrownBy(() -> client.sendInTransaction(message))
                .isInstanceOf(DuplicateMessageException.class)
                .hasMessageContaining("dup");
        verify(dispatcher, never()).dispatchById(anyLong());
        assertThat(transactionExecutor.callbacks).isEmpty();
    }

    @Test
    void payloadAtLimitIsAccepted() {
        OutboxMessage message = message();
        message.setPayload("a".repeat(65535));

        client.sendInTransaction(message);

        verify(repository).insert(message);
    }

    @Test
    void payloadOverLimitIsRejectedBeforeAnyDatabaseWork() {
        OutboxMessage message = message();
        message.setPayload("a".repeat(65536));

        assertThatThrownBy(() -> client.sendInTransaction(message))
                .isInstanceOf(MessageTooLargeException.class)
                .hasMessageContaining("65536")
                .hasMessageContaining("65535");
        verifyNoInteractions(repository);
    }

    @Test
    void headersAtLimitAreAcceptedAndOverLimitRejected() {
        OutboxMessage ok = message();
        ok.setHeaders("h".repeat(1024));
        client.sendInTransaction(ok);
        verify(repository).insert(ok);

        OutboxMessage tooLarge = message();
        tooLarge.setHeaders("h".repeat(1025));
        assertThatThrownBy(() -> client.sendInTransaction(tooLarge))
                .isInstanceOf(MessageTooLargeException.class)
                .hasMessageContaining("headers");
    }

    @Test
    void multiByteCharactersAreCountedByUtf8Bytes() {
        properties.getLimits().setPayloadBytes(6);
        OutboxMessage ok = message();
        ok.setPayload("中文");
        client.sendInTransaction(ok);
        verify(repository).insert(ok);

        OutboxMessage tooLarge = message();
        tooLarge.setPayload("中文a");
        assertThatThrownBy(() -> client.sendInTransaction(tooLarge))
                .isInstanceOf(MessageTooLargeException.class)
                .hasMessageContaining("actual=7");
    }

    @Test
    void rejectsInvalidFieldsAndMsgId() {
        OutboxMessage noBiz = message();
        noBiz.setBizCode(" ");
        assertThatThrownBy(() -> client.sendInTransaction(noBiz))
                .isInstanceOf(IllegalArgumentException.class);

        OutboxMessage badMsgId = message();
        badMsgId.setMsgId("bad id!");
        assertThatThrownBy(() -> client.sendInTransaction(badMsgId))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("msgId");
    }

    private static OutboxMessage message() {
        OutboxMessage message = new OutboxMessage();
        message.setBizCode("order");
        message.setTopic("order-events");
        message.setPartitionKey("order-1");
        message.setPayload("{}");
        return message;
    }

    private static final class ImmediateTransactionExecutor implements TransactionExecutor {
        private final List<Runnable> callbacks = new ArrayList<>();
        private Boolean usedExistingTransaction;

        @Override
        public <T> T executeInExistingTransaction(Supplier<T> action) {
            usedExistingTransaction = true;
            return run(action);
        }

        @Override
        public <T> T executeInNewTransaction(Supplier<T> action) {
            usedExistingTransaction = false;
            return run(action);
        }

        private <T> T run(Supplier<T> action) {
            T result = action.get();
            List<Runnable> pending = new ArrayList<>(callbacks);
            callbacks.clear();
            pending.forEach(Runnable::run);
            return result;
        }

        @Override
        public void afterCommit(Runnable action) {
            callbacks.add(action);
        }
    }
}
