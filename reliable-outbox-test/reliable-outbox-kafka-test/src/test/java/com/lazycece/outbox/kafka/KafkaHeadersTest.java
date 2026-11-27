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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lazycece.outbox.core.model.OutboxMessage;
import com.lazycece.outbox.core.spi.NonRetryablePublishException;
import java.nio.charset.StandardCharsets;
import java.util.stream.StreamSupport;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class KafkaHeadersTest {

    private final KafkaHeaders kafkaHeaders = new KafkaHeaders(new ObjectMapper());

    @Test
    void appliesScalarHeadersAndMsgId() {
        RecordHeaders headers = new RecordHeaders();

        kafkaHeaders.apply(message("m-1",
                "{\"traceId\":\"abc\",\"attempt\":2,\"flag\":true,\"skip\":null}"), headers);

        assertThat(header(headers, "traceId")).isEqualTo("abc");
        assertThat(header(headers, "attempt")).isEqualTo("2");
        assertThat(header(headers, "flag")).isEqualTo("true");
        assertThat(headers.lastHeader("skip")).isNull();
        assertThat(header(headers, KafkaHeaders.MSG_ID_HEADER)).isEqualTo("m-1");
    }

    @Test
    void blankHeadersOnlyAddMsgId() {
        RecordHeaders nullHeaders = new RecordHeaders();
        kafkaHeaders.apply(message("m-2", null), nullHeaders);

        RecordHeaders blankHeaders = new RecordHeaders();
        kafkaHeaders.apply(message("m-3", "   "), blankHeaders);

        assertThat(header(nullHeaders, KafkaHeaders.MSG_ID_HEADER)).isEqualTo("m-2");
        assertThat(StreamSupport.stream(nullHeaders.spliterator(), false).count()).isEqualTo(1);
        assertThat(header(blankHeaders, KafkaHeaders.MSG_ID_HEADER)).isEqualTo("m-3");
        assertThat(StreamSupport.stream(blankHeaders.spliterator(), false).count()).isEqualTo(1);
    }

    @Test
    void reservedMsgIdHeaderCannotBeOverridden() {
        RecordHeaders headers = new RecordHeaders();

        kafkaHeaders.apply(message("real-id",
                "{\"outbox-msg-id\":\"fake-id\",\"traceId\":\"abc\"}"), headers);

        assertThat(header(headers, KafkaHeaders.MSG_ID_HEADER)).isEqualTo("real-id");
        assertThat(StreamSupport.stream(headers.spliterator(), false)
                .filter(h -> KafkaHeaders.MSG_ID_HEADER.equals(h.key()))
                .count()).isEqualTo(1);
        assertThat(header(headers, "traceId")).isEqualTo("abc");
    }

    @ParameterizedTest
    @ValueSource(strings = {"{not-json", "[]", "\"123\"", "null", "{\"nested\":{\"a\":1}}"})
    void rejectsInvalidHeadersJson(String raw) {
        assertThatThrownBy(() -> kafkaHeaders.apply(message("m-4", raw), new RecordHeaders()))
                .isInstanceOf(NonRetryablePublishException.class)
                .hasMessageContaining("invalid outbox headers JSON");
    }

    private static OutboxMessage message(String msgId, String headers) {
        OutboxMessage message = new OutboxMessage();
        message.setMsgId(msgId);
        message.setHeaders(headers);
        return message;
    }

    private static String header(Headers headers, String name) {
        Header header = headers.lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
