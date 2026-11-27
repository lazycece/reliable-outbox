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
package com.lazycece.outbox.sample.rocketmq;

import com.lazycece.outbox.rocketmq.RocketMQHeaders;
import com.lazycece.outbox.sample.common.service.OrderService;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Consumes the sample topic back to prove end-to-end delivery.
 *
 * <p>The reserved {@code outbox-msg-id} user property is the consumer-side idempotency key:
 * delivery is at-least-once, so a real consumer must deduplicate on it.
 *
 * @author lazycece
 */
@Component
@ConditionalOnProperty(prefix = "sample.rocketmq", name = "consumer-enabled",
        havingValue = "true", matchIfMissing = true)
public class RocketMQOrderListener implements InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(RocketMQOrderListener.class);

    private final ConsumedEventStore store;
    private final String namesrvAddr;
    private final String consumerGroup;
    private DefaultMQPushConsumer consumer;

    public RocketMQOrderListener(
            ConsumedEventStore store,
            @Value("${reliable-outbox.rocketmq.namesrv-addr}") String namesrvAddr,
            @Value("${sample.rocketmq.consumer-group:reliable-outbox-sample-consumer}")
            String consumerGroup) {
        this.store = store;
        this.namesrvAddr = namesrvAddr;
        this.consumerGroup = consumerGroup;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.subscribe(OrderService.TOPIC, "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) -> {
            for (MessageExt message : messages) {
                String msgId = message.getUserProperty(RocketMQHeaders.MSG_ID_PROPERTY);
                store.add(new ConsumedEventStore.ConsumedEvent(
                        msgId,
                        message.getKeys(),
                        new String(message.getBody(), StandardCharsets.UTF_8),
                        Instant.now()));
                log.info("consumed order event, msgId={}, keys={}", msgId, message.getKeys());
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
    }

    @Override
    public void destroy() {
        if (consumer != null) {
            consumer.shutdown();
        }
    }
}
