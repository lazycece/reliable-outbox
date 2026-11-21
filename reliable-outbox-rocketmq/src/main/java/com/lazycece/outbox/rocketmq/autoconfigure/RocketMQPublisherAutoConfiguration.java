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
package com.lazycece.outbox.rocketmq.autoconfigure;

import com.lazycece.outbox.autoconfigure.ReliableOutboxAutoConfiguration;
import com.lazycece.outbox.core.spi.MessagePublisher;
import com.lazycece.outbox.rocketmq.RocketMQMessagePublisher;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configures a RocketMQ-backed {@link MessagePublisher}.
 *
 * <p>A {@code DefaultMQProducer} is created from {@code reliable-outbox.rocketmq.*} unless the
 * application provides its own producer bean. In that case the adapter reuses it and does not
 * manage its lifecycle.
 *
 * @author lazycece
 */
@AutoConfiguration(before = ReliableOutboxAutoConfiguration.class)
@ConditionalOnClass(DefaultMQProducer.class)
@ConditionalOnProperty(prefix = "reliable-outbox.rocketmq", name = "enabled",
        havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(RocketMQPublisherProperties.class)
public class RocketMQPublisherAutoConfiguration {

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean
    public DefaultMQProducer defaultMQProducer(RocketMQPublisherProperties properties)
            throws MQClientException {
        String namesrvAddr = properties.getNamesrvAddr();
        if (namesrvAddr == null || namesrvAddr.isBlank()) {
            throw new IllegalStateException("reliable-outbox.rocketmq.namesrv-addr must be set "
                    + "when no DefaultMQProducer bean is provided");
        }
        DefaultMQProducer producer = new DefaultMQProducer(properties.getProducerGroup());
        producer.setNamesrvAddr(namesrvAddr);
        producer.setSendMsgTimeout(
                (int) Math.min(Integer.MAX_VALUE, properties.getSendTimeout().toMillis()));
        producer.start();
        return producer;
    }

    @Bean
    @ConditionalOnMissingBean(MessagePublisher.class)
    public MessagePublisher rocketMQMessagePublisher(DefaultMQProducer producer,
            RocketMQPublisherProperties properties) {
        return new RocketMQMessagePublisher(producer, properties.getSendTimeout());
    }
}
