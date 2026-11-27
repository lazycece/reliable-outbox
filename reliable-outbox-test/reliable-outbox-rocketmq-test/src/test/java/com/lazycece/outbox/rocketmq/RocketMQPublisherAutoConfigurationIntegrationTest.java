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

import com.lazycece.outbox.autoconfigure.ReliableOutboxAutoConfiguration;
import com.lazycece.outbox.core.spi.MessagePublisher;
import com.lazycece.outbox.rocketmq.autoconfigure.RocketMQPublisherAutoConfiguration;
import javax.sql.DataSource;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.transaction.PlatformTransactionManager;

class RocketMQPublisherAutoConfigurationIntegrationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    RocketMQPublisherAutoConfiguration.class,
                    ReliableOutboxAutoConfiguration.class))
            .withBean(DataSource.class, () -> mock(DataSource.class))
            .withBean(PlatformTransactionManager.class,
                    () -> mock(PlatformTransactionManager.class))
            .withBean(DefaultMQProducer.class, () -> mock(DefaultMQProducer.class));

    @Test
    void rocketMQPublisherWinsOverMissingPublisherFallback() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(MessagePublisher.class);
            assertThat(context.getBean(MessagePublisher.class))
                    .isInstanceOf(RocketMQMessagePublisher.class);
        });
    }
}
