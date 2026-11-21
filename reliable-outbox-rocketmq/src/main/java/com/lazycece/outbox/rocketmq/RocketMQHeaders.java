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
package com.lazycece.outbox.rocketmq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lazycece.outbox.core.model.OutboxMessage;
import com.lazycece.outbox.core.spi.NonRetryablePublishException;
import java.util.Map;
import org.apache.rocketmq.common.message.Message;

/**
 * Maps the outbox {@code headers} JSON column to RocketMQ user properties.
 *
 * @author lazycece
 */
public final class RocketMQHeaders {

    /**
     * Reserved user property carrying the outbox message id for consumer idempotency.
     */
    public static final String MSG_ID_PROPERTY = "outbox-msg-id";

    private final ObjectMapper objectMapper;

    public RocketMQHeaders(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public void apply(OutboxMessage message, Message target) {
        String raw = message.getHeaders();
        if (raw != null && !raw.isBlank()) {
            parse(raw).forEach((key, value) -> {
                if (value != null && !MSG_ID_PROPERTY.equals(key)) {
                    putUserProperty(target, key, value);
                }
            });
        }
        putUserProperty(target, MSG_ID_PROPERTY, message.getMsgId());
    }

    private Map<String, String> parse(String raw) {
        try {
            Map<String, String> headers = objectMapper.readValue(
                    raw, new TypeReference<Map<String, String>>() {
                    });
            if (headers == null) {
                throw new NonRetryablePublishException("invalid outbox headers JSON", null);
            }
            return headers;
        } catch (JsonProcessingException ex) {
            throw new NonRetryablePublishException("invalid outbox headers JSON", ex);
        }
    }

    private void putUserProperty(Message target, String key, String value) {
        try {
            target.putUserProperty(key, value);
        } catch (RuntimeException ex) {
            throw new NonRetryablePublishException(
                    "outbox header '" + key
                            + "' cannot be set as a RocketMQ user property: " + ex.getMessage(),
                    ex);
        }
    }
}
