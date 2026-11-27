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
import static org.mockito.Mockito.mock;

import com.lazycece.outbox.core.spi.MessagePublisher;
import com.lazycece.outbox.kafka.autoconfigure.KafkaPublisherAutoConfiguration;
import com.lazycece.outbox.kafka.autoconfigure.KafkaPublisherProperties;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

class KafkaPublisherAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaPublisherAutoConfiguration.class))
            .withBean(KafkaTemplate.class, () -> mock(KafkaTemplate.class));

    @Test
    void registersKafkaPublisherWhenTemplatePresent() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(MessagePublisher.class);
            assertThat(context.getBean(MessagePublisher.class))
                    .isInstanceOf(KafkaMessagePublisher.class);
            assertThat(context.getBean(KafkaPublisherProperties.class).getSendTimeout())
                    .isEqualTo(Duration.ofSeconds(5));
        });
    }

    @Test
    void bindsCustomSendTimeout() {
        runner.withPropertyValues("reliable-outbox.kafka.send-timeout=5s")
                .run(context -> assertThat(
                        context.getBean(KafkaPublisherProperties.class).getSendTimeout())
                        .isEqualTo(Duration.ofSeconds(5)));
    }

    @Test
    void disabledPropertySkipsAdapter() {
        runner.withPropertyValues("reliable-outbox.kafka.enabled=false")
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
    void noPublisherWithoutKafkaTemplate() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KafkaPublisherAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(MessagePublisher.class);
                });
    }

    @Test
    void rejectsNonPositiveSendTimeout() {
        runner.withPropertyValues("reliable-outbox.kafka.send-timeout=0s")
                .run(context -> assertThat(context.getStartupFailure())
                        .hasStackTraceContaining(
                                "reliable-outbox.kafka.send-timeout must be greater than 0"));
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
