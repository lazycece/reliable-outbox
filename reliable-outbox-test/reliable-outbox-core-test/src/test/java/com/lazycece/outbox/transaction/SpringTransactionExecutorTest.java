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
package com.lazycece.outbox.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

/**
 * @author lazycece
 * @date 2026/11/11
 */
class SpringTransactionExecutorTest {

    private JdbcTemplate jdbcTemplate;
    private TransactionTemplate transactionTemplate;
    private SpringTransactionExecutor executor;

    @BeforeEach
    void setUp() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.execute("CREATE TABLE biz_order (id BIGINT PRIMARY KEY)");
        jdbcTemplate.execute("CREATE TABLE biz_event (id BIGINT PRIMARY KEY)");
        PlatformTransactionManager transactionManager = new DataSourceTransactionManager(dataSource);
        transactionTemplate = new TransactionTemplate(transactionManager);
        executor = new SpringTransactionExecutor(transactionManager);
    }

    @Test
    void mandatoryFailsWithoutAnExistingTransaction() {
        assertThatThrownBy(() -> executor.executeInExistingTransaction(() -> "x"))
                .isInstanceOf(IllegalTransactionStateException.class);
    }

    @Test
    void mandatoryJoinsTheExistingTransaction() {
        AtomicBoolean joined = new AtomicBoolean();
        transactionTemplate.executeWithoutResult(status -> {
            executor.executeInExistingTransaction(() -> {
                joined.set(true);
                return null;
            });
        });
        assertThat(joined).isTrue();
    }

    @Test
    void requiresNewSurvivesOuterTransactionRollback() {
        transactionTemplate.executeWithoutResult(status -> {
            jdbcTemplate.update("INSERT INTO biz_order (id) VALUES (1)");
            executor.executeInNewTransaction(() -> {
                jdbcTemplate.update("INSERT INTO biz_event (id) VALUES (1)");
                return null;
            });
            status.setRollbackOnly();
        });

        assertThat(count("biz_order")).isZero();
        assertThat(count("biz_event")).isEqualTo(1);
    }

    @Test
    void afterCommitRunsOnlyAfterCommit() {
        AtomicBoolean callback = new AtomicBoolean();
        transactionTemplate.executeWithoutResult(status -> {
            executor.afterCommit(() -> callback.set(true));
            assertThat(callback).isFalse();
        });
        assertThat(callback).isTrue();
    }

    @Test
    void afterCommitDoesNotRunOnRollback() {
        AtomicBoolean callback = new AtomicBoolean();
        transactionTemplate.executeWithoutResult(status -> {
            executor.afterCommit(() -> callback.set(true));
            status.setRollbackOnly();
        });
        assertThat(callback).isFalse();
    }

    @Test
    void afterCommitRequiresAnActiveTransaction() {
        assertThatThrownBy(() -> executor.afterCommit(() -> { }))
                .isInstanceOf(IllegalStateException.class);
    }

    private int count(String table) {
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + table, Integer.class);
        return count == null ? 0 : count;
    }
}
