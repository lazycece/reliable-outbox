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

import com.lazycece.outbox.config.ReliableOutboxProperties;
import com.lazycece.outbox.dispatch.MessageDispatcher;
import com.lazycece.outbox.model.OutboxMessage;
import com.lazycece.outbox.model.OutboxStatus;
import com.lazycece.outbox.repository.OutboxRepository;
import com.lazycece.outbox.transaction.TransactionExecutor;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lazycece
 * @date 2026/11/11
 */
public class DefaultMessageClient implements MessageClient {

    private static final Logger log = LoggerFactory.getLogger(DefaultMessageClient.class);

    private final OutboxRepository repository;
    private final MessageDispatcher dispatcher;
    private final TransactionExecutor transactionExecutor;
    private final ExecutorService dispatchExecutor;
    private final ReliableOutboxProperties properties;

    public DefaultMessageClient(OutboxRepository repository,
            MessageDispatcher dispatcher,
            TransactionExecutor transactionExecutor,
            ExecutorService dispatchExecutor,
            ReliableOutboxProperties properties) {
        this.repository = repository;
        this.dispatcher = dispatcher;
        this.transactionExecutor = transactionExecutor;
        this.dispatchExecutor = dispatchExecutor;
        this.properties = properties;
    }

    private enum SendMode {
        IN_TRANSACTION,
        STANDALONE
    }

    @Override
    public String sendInTransaction(OutboxMessage message) {
        return doSend(message, SendMode.IN_TRANSACTION);
    }

    @Override
    public String sendStandalone(OutboxMessage message) {
        return doSend(message, SendMode.STANDALONE);
    }

    private String doSend(OutboxMessage message, SendMode mode) {
        validate(message);

        Supplier<String> persistAction = () -> {
            prepare(message);
            return persistAndRegisterDispatch(message);
        };

        return switch (mode) {
            case IN_TRANSACTION -> transactionExecutor.executeInExistingTransaction(persistAction);
            case STANDALONE -> transactionExecutor.executeInNewTransaction(persistAction);
        };
    }

    private String persistAndRegisterDispatch(OutboxMessage message) {
        repository.insert(message);
        transactionExecutor.afterCommit(() -> submitDispatch(message.getId()));
        return message.getMsgId();
    }

    private void prepare(OutboxMessage message) {
        LocalDateTime now = LocalDateTime.now();
        if (message.getMsgId() == null) {
            message.setMsgId(UUID.randomUUID().toString().replace("-", ""));
        }
        message.setStatus(OutboxStatus.PENDING);
        message.setRetryCount(0);
        message.setCreatedAt(now);
        message.setUpdatedAt(now);
        message.setNextRetryTime(now.plus(properties.getDispatch().getInitialGrace()));
    }

    private void submitDispatch(Long id) {
        try {
            dispatchExecutor.execute(() -> {
                try {
                    dispatcher.dispatchById(id);
                } catch (Throwable ex) {
                    log.error("async dispatch failed, id={}", id, ex);
                }
            });
        } catch (RejectedExecutionException ex) {
            log.warn("dispatch queue full, id={} will be picked up by scanner", id);
        }
    }

    private void validate(OutboxMessage message) {
        Objects.requireNonNull(message, "message");
        requireText(message.getBizCode(), "bizCode");
        requireText(message.getTopic(), "topic");
        if (message.getPayload() == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        if (message.getBizCode().length() > 64) {
            throw new IllegalArgumentException("bizCode length must not exceed 64");
        }
        if (message.getTopic().length() > 255) {
            throw new IllegalArgumentException("topic length must not exceed 255");
        }
        if (message.getPartitionKey() != null && message.getPartitionKey().length() > 255) {
            throw new IllegalArgumentException("partitionKey length must not exceed 255");
        }
        if (message.getMsgId() != null
                && !message.getMsgId().matches("[A-Za-z0-9_-]{1,64}")) {
            throw new IllegalArgumentException(
                    "msgId must be 1-64 characters of ASCII letters, digits, underscore or hyphen");
        }
        int payloadBytes = message.getPayload().getBytes(StandardCharsets.UTF_8).length;
        if (payloadBytes > properties.getLimits().getPayloadBytes()) {
            throw new MessageTooLargeException("payload", payloadBytes,
                    properties.getLimits().getPayloadBytes());
        }
        if (message.getHeaders() != null) {
            int headerBytes = message.getHeaders().getBytes(StandardCharsets.UTF_8).length;
            if (headerBytes > properties.getLimits().getHeadersBytes()) {
                throw new MessageTooLargeException("headers", headerBytes,
                        properties.getLimits().getHeadersBytes());
            }
        }
    }

    private void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be empty");
        }
    }

}
