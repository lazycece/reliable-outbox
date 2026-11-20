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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lazycece.outbox.core.model.OutboxMessage;
import com.lazycece.outbox.core.spi.MessagePublisher;
import com.lazycece.outbox.core.spi.NonRetryablePublishException;
import com.lazycece.outbox.core.spi.PublishException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Publishes outbox messages to Kafka and waits for the broker acknowledgement.
 *
 * @author lazycece
 */
public final class KafkaMessagePublisher implements MessagePublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Duration sendTimeout;
    private final KafkaHeaders kafkaHeaders = new KafkaHeaders(new ObjectMapper());

    public KafkaMessagePublisher(KafkaTemplate<String, String> kafkaTemplate,
            Duration sendTimeout) {
        this.kafkaTemplate = kafkaTemplate;
        this.sendTimeout = Objects.requireNonNull(sendTimeout, "sendTimeout");
    }

    @Override
    public void publish(OutboxMessage message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                message.getTopic(), null, message.getPartitionKey(), message.getPayload());
        kafkaHeaders.apply(message, record.headers());
        try {
            kafkaTemplate.send(record).get(sendTimeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new PublishException("Kafka publish interrupted", ex);
        } catch (TimeoutException ex) {
            throw new PublishException("Kafka publish timed out after " + sendTimeout, ex);
        } catch (ExecutionException ex) {
            throw translate(ex.getCause());
        } catch (RuntimeException ex) {
            throw translate(ex);
        }
    }

    private PublishException translate(Throwable cause) {
        if (cause instanceof SerializationException
                || cause instanceof RecordTooLargeException
                || cause instanceof InvalidRecordException
                || cause instanceof InvalidTopicException) {
            return new NonRetryablePublishException(
                    "Kafka publish failed permanently: " + cause.getMessage(), cause);
        }
        return new PublishException("Kafka publish failed: " + cause.getMessage(), cause);
    }
}
