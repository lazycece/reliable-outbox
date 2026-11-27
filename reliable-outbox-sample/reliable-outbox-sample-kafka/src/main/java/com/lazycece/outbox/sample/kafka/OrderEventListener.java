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
package com.lazycece.outbox.sample.kafka;

import com.lazycece.outbox.kafka.KafkaHeaders;
import com.lazycece.outbox.sample.common.service.OrderService;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes the sample topic back to prove end-to-end delivery.
 *
 * <p>The reserved {@code outbox-msg-id} header is the consumer-side idempotency key: delivery
 * is at-least-once, so a real consumer must deduplicate on it.
 *
 * @author lazycece
 */
@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final ConsumedEventStore store;

    public OrderEventListener(ConsumedEventStore store) {
        this.store = store;
    }

    @KafkaListener(
            topics = OrderService.TOPIC,
            groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderEvent(ConsumerRecord<String, String> record) {
        String msgId = header(record, KafkaHeaders.MSG_ID_HEADER);
        store.add(new ConsumedEventStore.ConsumedEvent(
                msgId, record.key(), record.value(), Instant.now()));
        log.info("consumed order event, msgId={}, partitionKey={}", msgId, record.key());
    }

    private String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
