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
package com.lazycece.outbox.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.lazycece.outbox.client.MessageClient;
import com.lazycece.outbox.clean.OutboxCleaner;
import com.lazycece.outbox.deadletter.DeadLetterReplayer;
import com.lazycece.outbox.dispatch.MessageDispatcher;
import com.lazycece.outbox.model.OutboxMessage;
import com.lazycece.outbox.repository.OutboxRepository;
import com.lazycece.outbox.scan.MessageScanner;
import com.lazycece.outbox.spi.MessagePublisher;
import com.lazycece.outbox.transaction.TransactionExecutor;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * @author lazycece
 * @date 2026/11/11
 */
class ReliableOutboxAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ReliableOutboxAutoConfiguration.class))
            .withUserConfiguration(TestConfig.class);

    private final ApplicationContextRunner bootRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    DataSourceAutoConfiguration.class,
                    DataSourceTransactionManagerAutoConfiguration.class,
                    ReliableOutboxAutoConfiguration.class))
            .withUserConfiguration(PublisherOnlyConfig.class)
            .withPropertyValues(
                    "spring.datasource.url=jdbc:h2:mem:bootorder;DB_CLOSE_DELAY=-1",
                    "spring.datasource.driver-class-name=org.h2.Driver",
                    "spring.datasource.username=sa");

    @Test
    void registersAllBeans() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(OutboxRepository.class);
            assertThat(context).hasSingleBean(TransactionExecutor.class);
            assertThat(context).hasSingleBean(MessageDispatcher.class);
            assertThat(context).hasSingleBean(MessageClient.class);
            assertThat(context).hasSingleBean(MessageScanner.class);
            assertThat(context).hasSingleBean(OutboxCleaner.class);
            assertThat(context).hasSingleBean(DeadLetterReplayer.class);
            assertThat(context).hasBean("reliableOutboxDispatchExecutor");
        });
    }

    @Test
    void disabledPropertySkipsAutoConfiguration() {
        runner.withPropertyValues("reliable-outbox.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(MessageClient.class));
    }

    @Test
    void missingPublisherFailsWithClearMessage() {
        ApplicationContextRunner noPublisher = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ReliableOutboxAutoConfiguration.class))
                .withUserConfiguration(DataSourceConfig.class);
        noPublisher.run(context -> assertThat(context.getStartupFailure())
                .hasStackTraceContaining(
                        "reliable-outbox requires the application to provide a MessagePublisher bean"));
    }

    @Test
    void usesBootAutoConfiguredTransactionManager() {
        bootRunner.run(context -> {
            assertThat(context).hasSingleBean(PlatformTransactionManager.class);
            assertThat(context).hasSingleBean(TransactionExecutor.class);
            assertThat(context).hasSingleBean(MessageClient.class);
        });
    }

    @Test
    void dispatchesMessageAfterCommitEndToEnd() {
        bootRunner.run(context -> {
            JdbcTemplate jdbcTemplate = new JdbcTemplate(context.getBean(DataSource.class));
            initializeSchema(jdbcTemplate);

            RecordingPublisher publisher = (RecordingPublisher) context.getBean(MessagePublisher.class);
            MessageClient messageClient = context.getBean(MessageClient.class);
            PlatformTransactionManager transactionManager =
                    context.getBean(PlatformTransactionManager.class);

            OutboxMessage message = new OutboxMessage();
            message.setBizCode("order");
            message.setTopic("order-events");
            message.setPartitionKey("order-1");
            message.setPayload("{\"id\":1}");

            TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
            transactionTemplate.executeWithoutResult(status -> {
                messageClient.sendInTransaction(message);
                // 事务尚未提交，发布者不能被调用
                assertThat(publisher.received()).isEmpty();
            });

            assertThat(publisher.awaitPublish(Duration.ofSeconds(10)))
                    .as("publisher should be invoked after commit")
                    .isTrue();
            assertThat(publisher.received()).hasSize(1);
            OutboxMessage published = publisher.received().get(0);
            assertThat(published.getTopic()).isEqualTo("order-events");
            assertThat(published.getPayload()).isEqualTo("{\"id\":1}");

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                Map<String, Object> row = jdbcTemplate.queryForMap(
                        "SELECT status, sent_at FROM t_outbox WHERE msg_id = ?", message.getMsgId());
                assertThat(row.get("status")).isEqualTo("SENT");
                assertThat(row.get("sent_at")).isNotNull();
            });
        });
    }

    private static void initializeSchema(JdbcTemplate jdbcTemplate) {
        // Mirrors reliable-outbox-core/src/test/resources/schema-h2.sql.
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS t_outbox (
                    id BIGINT NOT NULL AUTO_INCREMENT,
                    msg_id VARCHAR(64) NOT NULL,
                    biz_code VARCHAR(64) NOT NULL,
                    topic VARCHAR(255) NOT NULL,
                    partition_key VARCHAR(255) DEFAULT NULL,
                    payload CLOB NOT NULL,
                    headers CLOB DEFAULT NULL,
                    status VARCHAR(16) NOT NULL DEFAULT 'INIT',
                    retry_count INT NOT NULL DEFAULT 0,
                    next_retry_time TIMESTAMP(3) DEFAULT NULL,
                    owner_token VARCHAR(64) DEFAULT NULL,
                    owner_expire_at TIMESTAMP(3) DEFAULT NULL,
                    sent_at TIMESTAMP(3) DEFAULT NULL,
                    last_error VARCHAR(1000) DEFAULT NULL,
                    created_at TIMESTAMP(3) NOT NULL,
                    updated_at TIMESTAMP(3) NOT NULL,
                    PRIMARY KEY (id),
                    CONSTRAINT uk_msg_id UNIQUE (msg_id)
                )
                """);
    }

    @Configuration(proxyBeanMethods = false)
    static class DataSourceConfig {
        @Bean
        DataSource dataSource() {
            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setURL("jdbc:h2:mem:autoconfig;DB_CLOSE_DELAY=-1");
            dataSource.setUser("sa");
            return dataSource;
        }

        @Bean
        PlatformTransactionManager transactionManager(DataSource dataSource) {
            return new DataSourceTransactionManager(dataSource);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @Import(DataSourceConfig.class)
    static class TestConfig {
        @Bean
        MessagePublisher messagePublisher() {
            return message -> { };
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PublisherOnlyConfig {
        @Bean
        RecordingPublisher messagePublisher() {
            return new RecordingPublisher();
        }
    }

    static final class RecordingPublisher implements MessagePublisher {
        private final List<OutboxMessage> messages = new CopyOnWriteArrayList<>();
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void publish(OutboxMessage message) {
            messages.add(message);
            latch.countDown();
        }

        List<OutboxMessage> received() {
            return messages;
        }

        boolean awaitPublish(Duration timeout) throws InterruptedException {
            return latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }
    }
}
