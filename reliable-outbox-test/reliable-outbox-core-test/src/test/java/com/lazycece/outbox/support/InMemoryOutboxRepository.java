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
package com.lazycece.outbox.support;

import com.lazycece.outbox.model.OutboxMessage;
import com.lazycece.outbox.repository.DuplicateMessageException;
import com.lazycece.outbox.repository.OutboxRepository;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author lazycece
 * @date 2026/11/11
 */
public class InMemoryOutboxRepository implements OutboxRepository {
    private final Map<Long, OutboxMessage> messages = new LinkedHashMap<>();
    private final AtomicLong sequence = new AtomicLong(1);

    public synchronized OutboxMessage put(OutboxMessage message) {
        OutboxMessage copy = copy(message);
        if (copy.getId() == null) {
            copy.setId(sequence.getAndIncrement());
        }
        messages.put(copy.getId(), copy);
        return copy(copy);
    }

    public synchronized OutboxMessage get(Long id) {
        OutboxMessage message = messages.get(id);
        return message == null ? null : copy(message);
    }

    public synchronized List<OutboxMessage> all() {
        return messages.values().stream().map(this::copy).toList();
    }

    @Override
    public synchronized void insert(OutboxMessage message) {
        if (findByMsgIdInternal(message.getMsgId()) != null) {
            throw new DuplicateMessageException(message.getMsgId());
        }
        OutboxMessage copy = copy(message);
        copy.setId(sequence.getAndIncrement());
        messages.put(copy.getId(), copy);
        message.setId(copy.getId());
    }

    @Override
    public synchronized OutboxMessage findById(Long id) {
        OutboxMessage message = messages.get(id);
        return message == null ? null : copy(message);
    }

    @Override
    public synchronized OutboxMessage findByMsgId(String msgId) {
        OutboxMessage message = findByMsgIdInternal(msgId);
        return message == null ? null : copy(message);
    }

    private OutboxMessage findByMsgIdInternal(String msgId) {
        return messages.values().stream()
                .filter(message -> Objects.equals(message.getMsgId(), msgId))
                .findFirst().orElse(null);
    }

    @Override
    public synchronized boolean tryAcquire(Long id, String ownerToken, LocalDateTime ownerExpireAt, LocalDateTime now) {
        OutboxMessage message = messages.get(id);
        if (!acquirable(message, now)) {
            return false;
        }
        message.setOwnerToken(ownerToken);
        message.setOwnerExpireAt(ownerExpireAt);
        message.setUpdatedAt(now);
        return true;
    }

    @Override
    public synchronized boolean tryAcquireDue(Long id, String ownerToken, LocalDateTime ownerExpireAt, LocalDateTime dueAt) {
        OutboxMessage message = messages.get(id);
        if (!acquirable(message, dueAt)) {
            return false;
        }
        if (message.getNextRetryTime() == null || message.getNextRetryTime().isAfter(dueAt)) {
            return false;
        }
        message.setOwnerToken(ownerToken);
        message.setOwnerExpireAt(ownerExpireAt);
        message.setUpdatedAt(dueAt);
        return true;
    }

    private boolean acquirable(OutboxMessage message, LocalDateTime now) {
        if (message == null || !message.getStatus().isDispatchable()) {
            return false;
        }
        return message.getOwnerToken() == null
                || message.getOwnerExpireAt() == null
                || message.getOwnerExpireAt().isBefore(now);
    }

    @Override
    public synchronized boolean releaseLease(Long id, String ownerToken, LocalDateTime now) {
        OutboxMessage message = messages.get(id);
        if (message == null || !Objects.equals(message.getOwnerToken(), ownerToken)) {
            return false;
        }
        message.setOwnerToken(null);
        message.setOwnerExpireAt(null);
        message.setUpdatedAt(now);
        return true;
    }

    @Override
    public synchronized boolean markSent(Long id, String ownerToken, LocalDateTime sentAt,
            int retryCount) {
        OutboxMessage message = messages.get(id);
        if (message == null || !Objects.equals(message.getOwnerToken(), ownerToken)
                || !message.getStatus().isDispatchable()) {
            return false;
        }
        message.setStatus(com.lazycece.outbox.model.OutboxStatus.SENT);
        message.setRetryCount(retryCount);
        message.setOwnerToken(null);
        message.setOwnerExpireAt(null);
        message.setSentAt(sentAt);
        message.setNextRetryTime(null);
        message.setLastError(null);
        message.setUpdatedAt(sentAt);
        return true;
    }

    @Override
    public synchronized boolean markFailed(Long id, String ownerToken, String error,
                                           LocalDateTime nextRetryTime, int retryCount,
                                           boolean dead, LocalDateTime now) {
        OutboxMessage message = messages.get(id);
        if (message == null || !Objects.equals(message.getOwnerToken(), ownerToken)
                || !message.getStatus().isDispatchable()) {
            return false;
        }
        message.setStatus(dead
                ? com.lazycece.outbox.model.OutboxStatus.DEAD
                : com.lazycece.outbox.model.OutboxStatus.PENDING);
        message.setOwnerToken(null);
        message.setOwnerExpireAt(null);
        message.setRetryCount(retryCount);
        message.setNextRetryTime(dead ? null : nextRetryTime);
        message.setLastError(error);
        message.setUpdatedAt(now);
        return true;
    }

    @Override
    public synchronized boolean resetForReplay(String msgId, LocalDateTime nextRetryTime, LocalDateTime now) {
        OutboxMessage message = findByMsgIdInternal(msgId);
        if (message == null || message.getStatus() != com.lazycece.outbox.model.OutboxStatus.DEAD) {
            return false;
        }
        message.setStatus(com.lazycece.outbox.model.OutboxStatus.PENDING);
        message.setRetryCount(0);
        message.setNextRetryTime(nextRetryTime);
        message.setOwnerToken(null);
        message.setOwnerExpireAt(null);
        message.setLastError(null);
        message.setUpdatedAt(now);
        return true;
    }

    @Override
    public synchronized List<Long> scanDispatchableIds(int limit, LocalDateTime now) {
        List<OutboxMessage> candidates = new ArrayList<>();
        for (OutboxMessage message : messages.values()) {
            if (message.getStatus().isDispatchable()
                    && message.getNextRetryTime() != null
                    && !message.getNextRetryTime().isAfter(now)
                    && acquirable(message, now)) {
                candidates.add(message);
            }
        }
        candidates.sort(Comparator
                .comparing(OutboxMessage::getNextRetryTime)
                .thenComparing(OutboxMessage::getId));
        return candidates.stream().limit(limit).map(OutboxMessage::getId).toList();
    }

    @Override
    public synchronized List<Long> scanSentBefore(LocalDateTime before, int limit) {
        return messages.values().stream()
                .filter(message -> message.getStatus() == com.lazycece.outbox.model.OutboxStatus.SENT)
                .filter(message -> message.getSentAt() != null && message.getSentAt().isBefore(before))
                .sorted(Comparator.comparing(OutboxMessage::getSentAt))
                .limit(limit)
                .map(OutboxMessage::getId)
                .toList();
    }

    @Override
    public synchronized int deleteSent(List<Long> ids) {
        int deleted = 0;
        for (Long id : ids) {
            OutboxMessage message = messages.get(id);
            if (message != null && message.getStatus() == com.lazycece.outbox.model.OutboxStatus.SENT) {
                messages.remove(id);
                deleted++;
            }
        }
        return deleted;
    }

    private OutboxMessage copy(OutboxMessage source) {
        OutboxMessage copy = new OutboxMessage();
        copy.setId(source.getId());
        copy.setMsgId(source.getMsgId());
        copy.setBizCode(source.getBizCode());
        copy.setTopic(source.getTopic());
        copy.setPartitionKey(source.getPartitionKey());
        copy.setPayload(source.getPayload());
        copy.setHeaders(source.getHeaders());
        copy.setStatus(source.getStatus());
        copy.setRetryCount(source.getRetryCount());
        copy.setNextRetryTime(source.getNextRetryTime());
        copy.setOwnerToken(source.getOwnerToken());
        copy.setOwnerExpireAt(source.getOwnerExpireAt());
        copy.setCreatedAt(source.getCreatedAt());
        copy.setUpdatedAt(source.getUpdatedAt());
        copy.setSentAt(source.getSentAt());
        copy.setLastError(source.getLastError());
        return copy;
    }
}
