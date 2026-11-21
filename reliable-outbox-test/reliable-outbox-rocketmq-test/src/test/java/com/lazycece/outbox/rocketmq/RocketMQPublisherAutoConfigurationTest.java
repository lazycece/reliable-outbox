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
import static org.mockito.Mockito.mock;

import com.lazycece.outbox.core.spi.MessagePublisher;
import com.lazycece.outbox.rocketmq.autoconfigure.RocketMQPublisherAutoConfiguration;
import com.lazycece.outbox.rocketmq.autoconfigure.RocketMQPublisherProperties;
import java.time.Duration;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

class RocketMQPublisherAutoConfigurationTest {

    private final DefaultMQProducer producer = mock(DefaultMQProducer.class);

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(RocketMQPublisherAutoConfiguration.class))
            .withBean(DefaultMQProducer.class, () -> producer);

    @Test
    void registersPublisherWhenProducerPresent() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(MessagePublisher.class);
            assertThat(context.getBean(MessagePublisher.class))
                    .isInstanceOf(RocketMQMessagePublisher.class);
            assertThat(context.getBean(DefaultMQProducer.class)).isSameAs(producer);
            assertThat(context.getBean(RocketMQPublisherProperties.class).getSendTimeout())
                    .isEqualTo(Duration.ofSeconds(5));
        });
    }

    @Test
    void bindsCustomSendTimeout() {
        runner.withPropertyValues("reliable-outbox.rocketmq.send-timeout=3s")
                .run(context -> assertThat(
                        context.getBean(RocketMQPublisherProperties.class).getSendTimeout())
                        .isEqualTo(Duration.ofSeconds(3)));
    }

    @Test
    void disabledPropertySkipsAdapter() {
        runner.withPropertyValues("reliable-outbox.rocketmq.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(MessagePublisher.class));
    }

    @Test
    void backsOffWhenCustomPublisherPresent() {
        runner.withUserConfiguration(CustomPublisherConfig.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(MessagePublisher.class);
                    assertThat(context.getBean(MessagePublisher.class))
                            .isSameAs(CustomPublisherConfig.PUBLISHER);
                });
    }

    @Test
    void rejectsNonPositiveSendTimeout() {
        runner.withPropertyValues("reliable-outbox.rocketmq.send-timeout=0s")
                .run(context -> assertThat(context.getStartupFailure())
                        .hasStackTraceContaining(
                                "reliable-outbox.rocketmq.send-timeout must be greater than 0"));
    }

    @Test
    void requiresNamesrvWhenCreatingProducer() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RocketMQPublisherAutoConfiguration.class))
                .run(context -> assertThat(context.getStartupFailure())
                        .hasStackTraceContaining(
                                "reliable-outbox.rocketmq.namesrv-addr must be set"));
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomPublisherConfig {

        static final MessagePublisher PUBLISHER = message -> {
        };

        @Bean
        MessagePublisher customPublisher() {
            return PUBLISHER;
        }
    }
}
