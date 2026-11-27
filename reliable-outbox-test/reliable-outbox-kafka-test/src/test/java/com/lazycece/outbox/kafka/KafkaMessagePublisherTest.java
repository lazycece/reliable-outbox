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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.lazycece.outbox.core.model.OutboxMessage;
import com.lazycece.outbox.core.spi.NonRetryablePublishException;
import com.lazycece.outbox.core.spi.PublishException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

@SuppressWarnings("unchecked")
class KafkaMessagePublisherTest {

    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);

    @Test
    void publishesRecordWithKeyPayloadAndHeaders() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        new KafkaMessagePublisher(kafkaTemplate, Duration.ofSeconds(10))
                .publish(message("m-1", "order-events", "order-1", "{\"id\":1}",
                        "{\"traceId\":\"abc\"}"));

        ArgumentCaptor<ProducerRecord<String, String>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, String> record = captor.getValue();
        assertThat(record.topic()).isEqualTo("order-events");
        assertThat(record.key()).isEqualTo("order-1");
        assertThat(record.value()).isEqualTo("{\"id\":1}");
        assertThat(header(record, "traceId")).isEqualTo("abc");
        assertThat(header(record, KafkaHeaders.MSG_ID_HEADER)).isEqualTo("m-1");
    }

    @Test
    void leavesKeyNullWhenPartitionKeyIsAbsent() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        new KafkaMessagePublisher(kafkaTemplate, Duration.ofSeconds(10))
                .publish(message("m-2", "order-events", null, "{}", null));

        ArgumentCaptor<ProducerRecord<String, String>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        assertThat(captor.getValue().key()).isNull();
        assertThat(header(captor.getValue(), KafkaHeaders.MSG_ID_HEADER)).isEqualTo("m-2");
    }

    @Test
    void malformedHeadersFailWithoutSending() {
        assertThatThrownBy(() -> new KafkaMessagePublisher(kafkaTemplate, Duration.ofSeconds(10))
                .publish(message("m-3", "order-events", "k", "{}", "{oops")))
                .isInstanceOf(NonRetryablePublishException.class);

        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
    }

    @Test
    void timeoutIsRetryable() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(new CompletableFuture<>());

        assertThatThrownBy(() -> new KafkaMessagePublisher(kafkaTemplate, Duration.ofMillis(50))
                .publish(message("m-4", "order-events", "k", "{}", null)))
                .isInstanceOf(PublishException.class)
                .isNotInstanceOf(NonRetryablePublishException.class)
                .hasMessageContaining("timed out");
    }

    @Test
    void interruptIsRestoredAndWrapped() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(new CompletableFuture<>());
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> new KafkaMessagePublisher(kafkaTemplate,
                    Duration.ofSeconds(10))
                    .publish(message("m-5", "order-events", "k", "{}", null)))
                    .isInstanceOf(PublishException.class)
                    .hasMessageContaining("interrupted");
            assertThat(Thread.interrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void synchronousRecordTooLargeIsNonRetryable() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenThrow(new RecordTooLargeException("too large"));

        assertThatThrownBy(() -> new KafkaMessagePublisher(kafkaTemplate, Duration.ofSeconds(10))
                .publish(message("m-6", "order-events", "k", "{}", null)))
                .isInstanceOf(NonRetryablePublishException.class)
                .hasCauseInstanceOf(RecordTooLargeException.class);
    }

    @Test
    void synchronousInvalidTopicIsNonRetryable() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenThrow(new InvalidTopicException("bad topic"));

        assertThatThrownBy(() -> new KafkaMessagePublisher(kafkaTemplate, Duration.ofSeconds(10))
                .publish(message("m-7", "order-events", "k", "{}", null)))
                .isInstanceOf(NonRetryablePublishException.class)
                .hasCauseInstanceOf(InvalidTopicException.class);
    }

    @Test
    void asynchronousSerializationFailureIsNonRetryable() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(
                        new SerializationException("bad payload")));

        assertThatThrownBy(() -> new KafkaMessagePublisher(kafkaTemplate, Duration.ofSeconds(10))
                .publish(message("m-8", "order-events", "k", "{}", null)))
                .isInstanceOf(NonRetryablePublishException.class)
                .hasCauseInstanceOf(SerializationException.class);
    }

    @Test
    void asynchronousRetryableFailureIsRetryable() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(
                        new org.apache.kafka.common.errors.TimeoutException("broker slow")));

        assertThatThrownBy(() -> new KafkaMessagePublisher(kafkaTemplate, Duration.ofSeconds(10))
                .publish(message("m-11", "order-events", "k", "{}", null)))
                .isInstanceOf(PublishException.class)
                .isNotInstanceOf(NonRetryablePublishException.class)
                .hasCauseInstanceOf(org.apache.kafka.common.errors.TimeoutException.class);
    }

    @Test
    void asynchronousInvalidRecordFailureIsNonRetryable() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(
                        new org.apache.kafka.common.InvalidRecordException("bad record")));

        assertThatThrownBy(() -> new KafkaMessagePublisher(kafkaTemplate, Duration.ofSeconds(10))
                .publish(message("m-10", "order-events", "k", "{}", null)))
                .isInstanceOf(NonRetryablePublishException.class)
                .hasCauseInstanceOf(org.apache.kafka.common.InvalidRecordException.class);
    }

    @Test
    void otherKafkaFailuresAreRetryable() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenThrow(new org.apache.kafka.common.errors.TimeoutException("broker timeout"));

        assertThatThrownBy(() -> new KafkaMessagePublisher(kafkaTemplate, Duration.ofSeconds(10))
                .publish(message("m-9", "order-events", "k", "{}", null)))
                .isInstanceOf(PublishException.class)
                .isNotInstanceOf(NonRetryablePublishException.class)
                .hasCauseInstanceOf(org.apache.kafka.common.errors.TimeoutException.class);
    }

    private static OutboxMessage message(String msgId, String topic, String key, String payload,
            String headers) {
        OutboxMessage message = new OutboxMessage();
        message.setMsgId(msgId);
        message.setTopic(topic);
        message.setPartitionKey(key);
        message.setPayload(payload);
        message.setHeaders(headers);
        return message;
    }

    private static String header(ProducerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
