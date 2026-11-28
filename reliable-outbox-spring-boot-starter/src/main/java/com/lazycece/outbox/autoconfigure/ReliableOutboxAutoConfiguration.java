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

import com.lazycece.outbox.core.client.DefaultMessageClient;
import com.lazycece.outbox.core.client.MessageClient;
import com.lazycece.outbox.core.clean.OutboxCleaner;
import com.lazycece.outbox.core.config.ReliableOutboxProperties;
import com.lazycece.outbox.core.deadletter.DeadLetterHandler;
import com.lazycece.outbox.core.deadletter.DeadLetterReplayer;
import com.lazycece.outbox.core.dispatch.MessageDispatcher;
import com.lazycece.outbox.core.dispatch.RetryPolicy;
import com.lazycece.outbox.core.repository.JdbcOutboxRepository;
import com.lazycece.outbox.core.repository.OutboxRepository;
import com.lazycece.outbox.core.scan.MessageScanner;
import com.lazycece.outbox.core.spi.LoggingMessageListener;
import com.lazycece.outbox.core.spi.MessageListener;
import com.lazycece.outbox.core.spi.MessagePublisher;
import com.lazycece.outbox.core.transaction.SpringTransactionExecutor;
import com.lazycece.outbox.core.transaction.TransactionExecutor;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * @author lazycece
 */
@AutoConfiguration(afterName = "org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration")
@EnableConfigurationProperties(ReliableOutboxProperties.class)
@ConditionalOnProperty(prefix = "reliable-outbox", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class ReliableOutboxAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(OutboxRepository.class)
    public JdbcOutboxRepository outboxRepository(DataSource dataSource,
            ReliableOutboxProperties properties) {
        return new JdbcOutboxRepository(new JdbcTemplate(dataSource), properties.getTableName());
    }

    @Bean(name = "reliableOutboxDispatchExecutor", destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "reliableOutboxDispatchExecutor")
    public ExecutorService reliableOutboxDispatchExecutor(ReliableOutboxProperties properties) {
        return new ThreadPoolExecutor(
                properties.getDispatch().getThreads(),
                properties.getDispatch().getThreads(),
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(properties.getDispatch().getQueueCapacity()),
                namedThreadFactory("reliable-outbox-dispatch-"),
                new ThreadPoolExecutor.AbortPolicy());
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnSingleCandidate(PlatformTransactionManager.class)
    public TransactionExecutor transactionExecutor(PlatformTransactionManager transactionManager) {
        return new SpringTransactionExecutor(transactionManager);
    }

    @Bean
    @ConditionalOnMissingBean
    public RetryPolicy retryPolicy(ReliableOutboxProperties properties) {
        return new RetryPolicy(properties.getRetry().getMaxCount(),
                properties.getRetry().getInitialInterval(),
                properties.getRetry().getMaxInterval(),
                properties.getRetry().getJitter());
    }

    @Bean
    @ConditionalOnMissingBean
    public MessageListener messageListener() {
        return new LoggingMessageListener();
    }

    @Bean
    @ConditionalOnMissingBean
    public DeadLetterHandler deadLetterHandler(MessageListener listener) {
        return new DeadLetterHandler(listener);
    }

    @Bean
    @ConditionalOnMissingBean(MessagePublisher.class)
    public MessagePublisher missingMessagePublisher() {
        throw new IllegalStateException(
                "reliable-outbox requires the application to provide a MessagePublisher bean");
    }

    @Bean
    @ConditionalOnMissingBean
    public MessageDispatcher messageDispatcher(OutboxRepository repository,
            MessagePublisher publisher,
            RetryPolicy retryPolicy,
            DeadLetterHandler deadLetterHandler,
            MessageListener listener,
            ReliableOutboxProperties properties) {
        return new MessageDispatcher(repository, publisher, retryPolicy,
                deadLetterHandler, listener, properties.getDispatch().getLeaseDuration());
    }

    @Bean
    @ConditionalOnMissingBean
    public MessageClient messageClient(OutboxRepository repository,
            MessageDispatcher dispatcher,
            TransactionExecutor transactionExecutor,
            @Qualifier("reliableOutboxDispatchExecutor") ExecutorService executor,
            ReliableOutboxProperties properties) {
        return new DefaultMessageClient(repository, dispatcher, transactionExecutor,
                executor, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public MessageScanner messageScanner(OutboxRepository repository,
            MessageDispatcher dispatcher,
            @Qualifier("reliableOutboxDispatchExecutor") ExecutorService executor,
            ReliableOutboxProperties properties) {
        return new MessageScanner(repository, dispatcher, executor, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxCleaner outboxCleaner(OutboxRepository repository,
            ReliableOutboxProperties properties) {
        return new OutboxCleaner(repository, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public DeadLetterReplayer deadLetterReplayer(OutboxRepository repository,
            MessageDispatcher dispatcher) {
        return new DeadLetterReplayer(repository, dispatcher);
    }

    private static ThreadFactory namedThreadFactory(String prefix) {
        AtomicInteger sequence = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
