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
package com.lazycece.outbox.core.model;

import java.time.LocalDateTime;

/**
 * @author lazycece
 */
public class OutboxMessage {

    /**
     * Auto-increment primary key.
     */
    private Long id;
    /**
     * Globally unique message ID, used as the business idempotency key.
     */
    private String msgId;
    /**
     * Business code identifying the source system or sending scenario.
     */
    private String bizCode;
    /**
     * Target message topic or queue.
     */
    private String topic;
    /**
     * Optional partition key; used only for broker routing and consumer grouping.
     */
    private String partitionKey;
    /**
     * Message payload, capped at 65535 bytes.
     */
    private String payload;
    /**
     * Passthrough metadata forwarded to the MQ as-is
     */
    private String headers;
    /**
     * Message status: PENDING / SENT / DEAD.
     */
    private OutboxStatus status;
    /**
     * Number of retries performed, excluding the first delivery attempt.
     */
    private int retryCount;
    /**
     * Earliest time the scanner may pick the message; null for SENT / DEAD.
     */
    private LocalDateTime nextRetryTime;
    /**
     * Owner token of the current delivery lease, used for concurrency control and recovery.
     */
    private String ownerToken;
    /**
     * Lease expiration time; after this the message may be acquired by another instance.
     */
    private LocalDateTime ownerExpireAt;
    /**
     * Delivery success time; only set for SENT messages.
     */
    private LocalDateTime sentAt;
    /**
     * Error message of the most recent delivery failure.
     */
    private String lastError;
    /**
     * Record creation time.
     */
    private LocalDateTime createdAt;
    /**
     * Record update time.
     */
    private LocalDateTime updatedAt;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getBizCode() {
        return bizCode;
    }

    public void setBizCode(String bizCode) {
        this.bizCode = bizCode;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getHeaders() {
        return headers;
    }

    public void setHeaders(String headers) {
        this.headers = headers;
    }

    public OutboxStatus getStatus() {
        return status;
    }

    public void setStatus(OutboxStatus status) {
        this.status = status;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public LocalDateTime getNextRetryTime() {
        return nextRetryTime;
    }

    public void setNextRetryTime(LocalDateTime nextRetryTime) {
        this.nextRetryTime = nextRetryTime;
    }

    public String getOwnerToken() {
        return ownerToken;
    }

    public void setOwnerToken(String ownerToken) {
        this.ownerToken = ownerToken;
    }

    public LocalDateTime getOwnerExpireAt() {
        return ownerExpireAt;
    }

    public void setOwnerExpireAt(LocalDateTime ownerExpireAt) {
        this.ownerExpireAt = ownerExpireAt;
    }

    public LocalDateTime getSentAt() {
        return sentAt;
    }

    public void setSentAt(LocalDateTime sentAt) {
        this.sentAt = sentAt;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
}
