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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lazycece.outbox.core.model.OutboxMessage;
import com.lazycece.outbox.core.spi.NonRetryablePublishException;
import org.apache.rocketmq.common.message.Message;
import org.junit.jupiter.api.Test;

class RocketMQHeadersTest {

    private final RocketMQHeaders headers = new RocketMQHeaders(new ObjectMapper());

    @Test
    void appliesScalarHeadersAndMsgId() {
        Message target = new Message();

        headers.apply(message("m-1",
                "{\"traceId\":\"abc\",\"attempt\":2,\"flag\":true,\"skip\":null}"), target);

        assertThat(target.getProperty("traceId")).isEqualTo("abc");
        assertThat(target.getProperty("attempt")).isEqualTo("2");
        assertThat(target.getProperty("flag")).isEqualTo("true");
        assertThat(target.getProperty("skip")).isNull();
        assertThat(target.getProperty(RocketMQHeaders.MSG_ID_PROPERTY)).isEqualTo("m-1");
    }

    @Test
    void blankHeadersOnlySetMsgId() {
        Message nullHeaders = new Message();
        headers.apply(message("m-2", null), nullHeaders);

        Message blankHeaders = new Message();
        headers.apply(message("m-3", "   "), blankHeaders);

        assertThat(nullHeaders.getProperties())
                .containsOnlyKeys(RocketMQHeaders.MSG_ID_PROPERTY);
        assertThat(blankHeaders.getProperties())
                .containsOnlyKeys(RocketMQHeaders.MSG_ID_PROPERTY);
    }

    @Test
    void reservedMsgIdPropertyCannotBeOverridden() {
        Message target = new Message();

        headers.apply(message("real-id",
                "{\"outbox-msg-id\":\"fake-id\",\"traceId\":\"abc\"}"), target);

        assertThat(target.getProperty(RocketMQHeaders.MSG_ID_PROPERTY)).isEqualTo("real-id");
        assertThat(target.getProperty("traceId")).isEqualTo("abc");
    }

    @Test
    void rejectsMalformedJson() {
        assertThatThrownBy(() -> headers.apply(message("m-4", "{not-json"),
                new Message()))
                .isInstanceOf(NonRetryablePublishException.class)
                .hasMessageContaining("invalid outbox headers JSON");
    }

    @Test
    void rejectsNonObjectJson() {
        assertThatThrownBy(() -> headers.apply(message("m-5", "[]"), new Message()))
                .isInstanceOf(NonRetryablePublishException.class)
                .hasMessageContaining("invalid outbox headers JSON");
    }

    @Test
    void rejectsNestedValue() {
        assertThatThrownBy(() -> headers.apply(message("m-6", "{\"nested\":{\"a\":1}}"),
                new Message()))
                .isInstanceOf(NonRetryablePublishException.class)
                .hasMessageContaining("invalid outbox headers JSON");
    }

    @Test
    void rejectsReservedUserPropertyName() {
        assertThatThrownBy(() -> headers.apply(message("m-7", "{\"TAGS\":\"x\"}"),
                new Message()))
                .isInstanceOf(NonRetryablePublishException.class)
                .hasMessageContaining("cannot be set as a RocketMQ user property");
    }

    private static OutboxMessage message(String msgId, String headers) {
        OutboxMessage message = new OutboxMessage();
        message.setMsgId(msgId);
        message.setHeaders(headers);
        return message;
    }
}
