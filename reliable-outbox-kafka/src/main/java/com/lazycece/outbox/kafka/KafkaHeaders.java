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
package com.lazycece.outbox.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lazycece.outbox.core.model.OutboxMessage;
import com.lazycece.outbox.core.spi.NonRetryablePublishException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.header.Headers;

/**
 * Maps the outbox {@code headers} JSON column to Kafka record headers.
 *
 * @author lazycece
 */
public final class KafkaHeaders {

    /**
     * Reserved header carrying the outbox message id for consumer idempotency.
     */
    public static final String MSG_ID_HEADER = "outbox-msg-id";

    private final ObjectMapper objectMapper;

    public KafkaHeaders(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public void apply(OutboxMessage message, Headers headers) {
        String raw = message.getHeaders();
        if (raw != null && !raw.isBlank()) {
            for (Map.Entry<String, String> header : parse(raw).entrySet()) {
                if (header.getValue() != null && !MSG_ID_HEADER.equals(header.getKey())) {
                    headers.add(header.getKey(),
                            header.getValue().getBytes(StandardCharsets.UTF_8));
                }
            }
        }
        headers.add(MSG_ID_HEADER, message.getMsgId().getBytes(StandardCharsets.UTF_8));
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
}
