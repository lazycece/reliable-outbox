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
package com.lazycece.outbox.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.lazycece.outbox.core.model.OutboxMessage;
import com.lazycece.outbox.core.model.OutboxStatus;
import com.lazycece.outbox.core.repository.DuplicateMessageException;
import com.lazycece.outbox.core.repository.JdbcOutboxRepository;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

/**
 * @author lazycece
 * @date 2026/11/11
 */
class JdbcOutboxRepositoryTest {

    /**
     * Columns are DATETIME(3)/TIMESTAMP(3), so values with sub-millisecond precision
     * are rounded by the database and do not round-trip exactly.
     */
    private static final Comparator<LocalDateTime> WITHIN_ONE_MILLI = (a, b) ->
            Duration.between(a, b).abs().compareTo(Duration.ofMillis(1)) <= 0
                    ? 0 : a.compareTo(b);

    private JdbcTemplate jdbcTemplate;
    private JdbcOutboxRepository repository;

    @BeforeEach
    void setUp() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + UUID.randomUUID()
                + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        DatabasePopulatorUtils.execute(
                new ResourceDatabasePopulator(new ClassPathResource("schema-h2.sql")), dataSource);
        jdbcTemplate = new JdbcTemplate(dataSource);
        repository = new JdbcOutboxRepository(jdbcTemplate, "t_outbox");
    }

    private OutboxMessage message(String msgId) {
        LocalDateTime now = LocalDateTime.now();
        OutboxMessage message = new OutboxMessage();
        message.setMsgId(msgId);
        message.setBizCode("order");
        message.setTopic("order-events");
        message.setPartitionKey("order-1");
        message.setPayload("{\"id\":1}");
        message.setHeaders("{}");
        message.setStatus(OutboxStatus.PENDING);
        message.setRetryCount(0);
        message.setNextRetryTime(now.plusSeconds(30));
        message.setCreatedAt(now);
        message.setUpdatedAt(now);
        return message;
    }

    @Test
    void insertFillsIdAndRejectsDuplicateMsgId() {
        OutboxMessage message = message("m1");
        repository.insert(message);
        assertThat(message.getId()).isNotNull();

        OutboxMessage duplicate = message("m1");
        assertThatThrownBy(() -> repository.insert(duplicate))
                .isInstanceOf(DuplicateMessageException.class)
                .hasMessageContaining("m1");
        assertThat(duplicate.getId()).isNull();
    }

    @Test
    void findByIdAndByMsgIdRoundTripAllFields() {
        OutboxMessage message = message("m1");
        repository.insert(message);

        OutboxMessage byId = repository.findById(message.getId());
        assertThat(byId).usingRecursiveComparison()
                .withComparatorForType(WITHIN_ONE_MILLI, LocalDateTime.class)
                .isEqualTo(message);
        assertThat(repository.findByMsgId("m1")).usingRecursiveComparison()
                .withComparatorForType(WITHIN_ONE_MILLI, LocalDateTime.class)
                .isEqualTo(message);
        assertThat(repository.findById(-1L)).isNull();
        assertThat(repository.findByMsgId("missing")).isNull();
    }

    @Test
    void tryAcquireRespectsStatusAndLease() {
        OutboxMessage message = message("m1");
        repository.insert(message);
        LocalDateTime now = LocalDateTime.now();

        assertThat(repository.tryAcquire(message.getId(), "t1", now.plusSeconds(60), now)).isTrue();
        assertThat(repository.tryAcquire(message.getId(), "t2", now.plusSeconds(60), now)).isFalse();
        assertThat(repository.tryAcquire(message.getId(), "t2",
                now.plusSeconds(60), now.plusSeconds(61))).isTrue();
        assertThat(repository.findById(message.getId()).getOwnerToken()).isEqualTo("t2");
    }

    @Test
    void tryAcquireDueOnlyAfterNextRetryTime() {
        OutboxMessage message = message("m1");
        repository.insert(message);
        // The stored value may be rounded up by less than a millisecond.
        LocalDateTime due = message.getNextRetryTime().plusNanos(1_000_000);

        assertThat(repository.tryAcquireDue(message.getId(), "t1",
                due.plusSeconds(60), due.minusSeconds(1))).isFalse();
        assertThat(repository.tryAcquireDue(message.getId(), "t1",
                due.plusSeconds(60), due)).isTrue();
    }

    @Test
    void releaseLeaseRequiresMatchingToken() {
        OutboxMessage message = message("m1");
        repository.insert(message);
        LocalDateTime now = LocalDateTime.now();
        repository.tryAcquire(message.getId(), "owner", now.plusSeconds(60), now);

        assertThat(repository.releaseLease(message.getId(), "other", now)).isFalse();
        assertThat(repository.releaseLease(message.getId(), "owner", now)).isTrue();
        assertThat(repository.findById(message.getId()).getOwnerToken()).isNull();
    }

    @Test
    void markSentRequiresLeaseOwner() {
        OutboxMessage message = message("m1");
        repository.insert(message);
        LocalDateTime now = LocalDateTime.now();
        repository.tryAcquire(message.getId(), "owner", now.plusSeconds(60), now);

        assertThat(repository.markSent(message.getId(), "other", now, 0)).isFalse();
        assertThat(repository.markSent(message.getId(), "owner", now, 2)).isTrue();

        OutboxMessage sent = repository.findById(message.getId());
        assertThat(sent.getStatus()).isEqualTo(OutboxStatus.SENT);
        assertThat(sent.getOwnerToken()).isNull();
        assertThat(sent.getNextRetryTime()).isNull();
        assertThat(sent.getLastError()).isNull();
        assertThat(sent.getRetryCount()).isEqualTo(2);
        assertWithinOneMilli(sent.getSentAt(), now);
    }

    @Test
    void markFailedWritesRetryOrDeadState() {
        OutboxMessage first = message("m1");
        repository.insert(first);
        LocalDateTime now = LocalDateTime.now();
        repository.tryAcquire(first.getId(), "owner", now.plusSeconds(60), now);
        LocalDateTime next = now.plusSeconds(30);

        assertThat(repository.markFailed(first.getId(), "owner", "boom", next, 1, false, now)).isTrue();
        OutboxMessage failed = repository.findById(first.getId());
        assertThat(failed.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(failed.getRetryCount()).isEqualTo(1);
        assertWithinOneMilli(failed.getNextRetryTime(), next);
        assertThat(failed.getLastError()).isEqualTo("boom");
        assertThat(failed.getOwnerToken()).isNull();

        repository.tryAcquire(first.getId(), "owner2", now.plusSeconds(60), now);
        assertThat(repository.markFailed(first.getId(), "owner2", "fatal", null, 16, true, now)).isTrue();
        OutboxMessage dead = repository.findById(first.getId());
        assertThat(dead.getStatus()).isEqualTo(OutboxStatus.DEAD);
        assertThat(dead.getRetryCount()).isEqualTo(16);
        assertThat(dead.getNextRetryTime()).isNull();
        assertThat(dead.getLastError()).isEqualTo("fatal");
    }

    @Test
    void resetForReplayOnlyResetsDeadMessages() {
        OutboxMessage message = message("m1");
        repository.insert(message);
        jdbcTemplate.update("UPDATE t_outbox SET status = 2, retry_count = 16 WHERE id = ?",
                message.getId());

        LocalDateTime now = LocalDateTime.now();
        assertThat(repository.resetForReplay("m1", now, now)).isTrue();
        OutboxMessage reset = repository.findById(message.getId());
        assertThat(reset.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(reset.getRetryCount()).isZero();
        assertWithinOneMilli(reset.getNextRetryTime(), now);
        assertThat(repository.resetForReplay("m1", now, now)).isFalse();
        assertThat(repository.resetForReplay("missing", now, now)).isFalse();
    }

    @Test
    void scanDispatchableIdsFiltersFutureAndLeasedRows() {
        LocalDateTime now = LocalDateTime.now();
        OutboxMessage due1 = message("due1");
        due1.setNextRetryTime(now.minusSeconds(10));
        OutboxMessage due2 = message("due2");
        due2.setNextRetryTime(now.minusSeconds(5));
        OutboxMessage future = message("future");
        future.setNextRetryTime(now.plusSeconds(60));
        repository.insert(due1);
        repository.insert(due2);
        repository.insert(future);

        List<Long> ids = repository.scanDispatchableIds(10, now);
        assertThat(ids).containsExactly(due1.getId(), due2.getId());

        repository.tryAcquire(due1.getId(), "owner", now.plusSeconds(60), now);
        assertThat(repository.scanDispatchableIds(10, now)).containsExactly(due2.getId());
    }

    @Test
    void scanAndDeleteOnlyTouchSentMessagesBeforeRetention() {
        LocalDateTime now = LocalDateTime.now();
        OutboxMessage old1 = message("old1");
        OutboxMessage old2 = message("old2");
        OutboxMessage recent = message("recent");
        OutboxMessage dead = message("dead");
        repository.insert(old1);
        repository.insert(old2);
        repository.insert(recent);
        repository.insert(dead);

        LocalDateTime oldSentAt = now.minusSeconds(8 * 24 * 3600L);
        jdbcTemplate.update("UPDATE t_outbox SET status = 1, sent_at = ? WHERE id = ?",
                oldSentAt, old1.getId());
        jdbcTemplate.update("UPDATE t_outbox SET status = 1, sent_at = ? WHERE id = ?",
                oldSentAt.plusNanos(1_000_000), old2.getId());
        jdbcTemplate.update("UPDATE t_outbox SET status = 1, sent_at = ? WHERE id = ?",
                now, recent.getId());
        jdbcTemplate.update("UPDATE t_outbox SET status = 2, sent_at = ? WHERE id = ?",
                oldSentAt, dead.getId());

        List<Long> candidates = repository.scanSentBetween(
                now.minusSeconds(10 * 24 * 3600L), now.minusSeconds(24 * 3600L), 10);
        assertThat(candidates).containsExactly(old1.getId(), old2.getId());
        assertThat(repository.deleteSent(candidates)).isEqualTo(2);
        assertThat(repository.findById(old1.getId())).isNull();
        assertThat(repository.findById(recent.getId()).getStatus()).isEqualTo(OutboxStatus.SENT);
        assertThat(repository.findById(dead.getId()).getStatus()).isEqualTo(OutboxStatus.DEAD);
        assertThat(repository.deleteSent(List.of())).isZero();
    }

    @Test
    void tableNameIsValidatedBeforeUse() {
        assertThatThrownBy(() -> new JdbcOutboxRepository(jdbcTemplate, "t_outbox; DROP TABLE x"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new JdbcOutboxRepository(jdbcTemplate, "1abc"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new JdbcOutboxRepository(jdbcTemplate, (String) null))
                .isInstanceOf(IllegalArgumentException.class);
        new JdbcOutboxRepository(jdbcTemplate, "biz_event_outbox");
    }

    @Test
    void minSentAtAndScanSentBetweenRespectStatusAndBounds() {
        LocalDateTime base = LocalDateTime.of(2026, 1, 1, 0, 0);
        OutboxMessage a = message("a");
        OutboxMessage b = message("b");
        OutboxMessage c = message("c");
        OutboxMessage dead = message("dead");
        repository.insert(a);
        repository.insert(b);
        repository.insert(c);
        repository.insert(dead);

        jdbcTemplate.update("UPDATE t_outbox SET status = 1, sent_at = ? WHERE id = ?",
                base, a.getId());
        jdbcTemplate.update("UPDATE t_outbox SET status = 1, sent_at = ? WHERE id = ?",
                base.plusSeconds(1), b.getId());
        jdbcTemplate.update("UPDATE t_outbox SET status = 1, sent_at = ? WHERE id = ?",
                base.plusSeconds(2), c.getId());
        jdbcTemplate.update("UPDATE t_outbox SET status = 2, sent_at = ? WHERE id = ?",
                base, dead.getId());

        assertWithinOneMilli(repository.minSentAt(), base);

        // [from, to): 含 from，不含 to
        assertThat(repository.scanSentBetween(base, base.plusSeconds(2), 10))
                .containsExactly(a.getId(), b.getId());
        assertThat(repository.scanSentBetween(base.plusSeconds(2), base.plusSeconds(3), 10))
                .containsExactly(c.getId());
        assertThat(repository.scanSentBetween(base.plusSeconds(3), base.plusSeconds(4), 10))
                .isEmpty();
    }

    @Test
    void minSentAtIsNullWhenNoSentMessages() {
        repository.insert(message("pending"));
        assertThat(repository.minSentAt()).isNull();
    }

    private static void assertWithinOneMilli(LocalDateTime actual, LocalDateTime expected) {
        assertThat(Duration.between(expected, actual).abs())
                .isLessThanOrEqualTo(Duration.ofMillis(1));
    }
}
