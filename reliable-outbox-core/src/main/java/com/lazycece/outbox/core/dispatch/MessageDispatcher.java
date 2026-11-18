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
package com.lazycece.outbox.core.dispatch;

import com.lazycece.outbox.core.deadletter.DeadLetterHandler;
import com.lazycece.outbox.core.model.OutboxMessage;
import com.lazycece.outbox.core.model.OutboxStatus;
import com.lazycece.outbox.core.repository.OutboxRepository;
import com.lazycece.outbox.core.spi.MessageListener;
import com.lazycece.outbox.core.spi.MessagePublisher;
import com.lazycece.outbox.core.spi.NonRetryablePublishException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lazycece
 * @date 2026/11/11
 */
public class MessageDispatcher {

    private static final Logger log = LoggerFactory.getLogger(MessageDispatcher.class);

    private final OutboxRepository repository;
    private final MessagePublisher publisher;
    private final RetryPolicy retryPolicy;
    private final DeadLetterHandler deadLetterHandler;
    private final MessageListener listener;
    private final Duration leaseDuration;

    public MessageDispatcher(OutboxRepository repository,
            MessagePublisher publisher,
            RetryPolicy retryPolicy,
            DeadLetterHandler deadLetterHandler,
            MessageListener listener,
            Duration leaseDuration) {
        this.repository = repository;
        this.publisher = publisher;
        this.retryPolicy = retryPolicy;
        this.deadLetterHandler = deadLetterHandler;
        this.listener = listener;
        this.leaseDuration = leaseDuration;
    }

    public void dispatchById(Long id) {
        LocalDateTime now = LocalDateTime.now();
        String ownerToken = UUID.randomUUID().toString();
        if (!repository.tryAcquire(id, ownerToken, now.plus(leaseDuration), now)) {
            return;
        }
        dispatchAcquired(id, ownerToken);
    }

    /**
     * The caller must already hold the lease for this message.
     */
    public void dispatchAcquired(Long id, String ownerToken) {
        OutboxMessage fresh = repository.findById(id);
        if (fresh == null) {
            log.warn("outbox message not found, id={}", id);
            return;
        }
        LocalDateTime now = LocalDateTime.now();
        if (!ownerToken.equals(fresh.getOwnerToken())) {
            log.warn("lease lost before publish, skip, id={}, msgId={}", id, fresh.getMsgId());
            safeNotify(() -> listener.onLeaseLost(fresh, "lease-lost-before-publish"));
            return;
        }
        if (fresh.getOwnerExpireAt() == null || !fresh.getOwnerExpireAt().isAfter(now)) {
            log.warn("lease expired before publish, release, id={}, msgId={}",
                    id, fresh.getMsgId());
            repository.releaseLease(id, ownerToken, now);
            return;
        }
        try {
            publisher.publish(fresh);
            LocalDateTime sentAt = LocalDateTime.now();
            int retriesPerformed = fresh.getRetryCount() + (isRetryAttempt(fresh) ? 1 : 0);
            if (!repository.markSent(id, ownerToken, sentAt, retriesPerformed)) {
                log.warn(
                        "markSent failed, lease may be lost, duplicate delivery possible, msgId={}",
                        fresh.getMsgId());
                safeNotify(() -> listener.onLeaseLost(fresh, "mark-sent-failed"));
                return;
            }
            fresh.setStatus(OutboxStatus.SENT);
            fresh.setSentAt(sentAt);
            fresh.setRetryCount(retriesPerformed);
            safeNotify(() -> listener.onSent(fresh));
        } catch (NonRetryablePublishException ex) {
            fail(fresh, ownerToken, ex, true);
        } catch (Exception ex) {
            fail(fresh, ownerToken, ex, false);
        }
    }

    private void fail(OutboxMessage message, String ownerToken, Exception error,
            boolean permanent) {
        int retryCount = isRetryAttempt(message) ? message.getRetryCount() + 1 : 0;
        boolean dead = permanent || retryPolicy.isDead(retryCount);
        LocalDateTime nextRetryTime = dead ? null : retryPolicy.nextRetryTime(retryCount);
        // A retry attempt is identified by a non-null last_error, so the first failure must
        // always persist something even when the exception has no message.
        String errorMessage = truncate(error.getMessage());
        if (errorMessage == null) {
            errorMessage = error.getClass().getName();
        }

        boolean updated = repository.markFailed(message.getId(), ownerToken,
                errorMessage, nextRetryTime, retryCount, dead,
                LocalDateTime.now());
        if (!updated) {
            log.warn("markFailed ignored, lease lost, msgId={}", message.getMsgId());
            return;
        }

        message.setRetryCount(retryCount);
        message.setLastError(errorMessage);
        message.setStatus(dead ? OutboxStatus.DEAD : OutboxStatus.PENDING);
        message.setNextRetryTime(nextRetryTime);

        if (dead) {
            deadLetterHandler.onDead(message);
            return;
        }

        safeNotify(() -> listener.onFailed(message, retryCount, error));
    }

    /**
     * A message is a retry attempt when it already failed at least once. Fresh and replayed
     * messages have a null last_error and start a full retry budget.
     */
    private static boolean isRetryAttempt(OutboxMessage message) {
        return message.getLastError() != null;
    }

    private void safeNotify(Runnable action) {
        try {
            action.run();
        } catch (Throwable ex) {
            log.warn("message listener failed", ex);
        }
    }

    private String truncate(String error) {
        if (error == null) {
            return null;
        }
        return error.length() > 1000 ? error.substring(0, 1000) : error;
    }
}
