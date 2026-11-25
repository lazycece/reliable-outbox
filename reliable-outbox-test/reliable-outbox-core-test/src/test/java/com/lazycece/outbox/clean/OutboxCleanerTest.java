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
package com.lazycece.outbox.clean;

import static org.assertj.core.api.Assertions.assertThat;

import com.lazycece.outbox.core.config.ReliableOutboxProperties;
import com.lazycece.outbox.core.clean.OutboxCleaner;
import com.lazycece.outbox.core.model.OutboxMessage;
import com.lazycece.outbox.core.model.OutboxStatus;
import com.lazycece.outbox.support.InMemoryOutboxRepository;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author lazycece
 */
class OutboxCleanerTest {

    private InMemoryOutboxRepository repository;
    private ReliableOutboxProperties properties;
    private OutboxCleaner cleaner;

    @BeforeEach
    void setUp() {
        repository = new InMemoryOutboxRepository();
        properties = new ReliableOutboxProperties();
        properties.getClean().setRetention(java.time.Duration.ofDays(7));
        properties.getClean().setBatchSize(2);
        properties.getClean().setMaxBatchesPerRun(2);
        cleaner = new OutboxCleaner(repository, properties);
    }

    private OutboxMessage seedSent(String msgId, LocalDateTime sentAt) {
        LocalDateTime now = LocalDateTime.now();
        OutboxMessage message = new OutboxMessage();
        message.setMsgId(msgId);
        message.setBizCode("order");
        message.setTopic("order-events");
        message.setPayload("{}");
        message.setStatus(OutboxStatus.SENT);
        message.setSentAt(sentAt);
        message.setCreatedAt(now);
        message.setUpdatedAt(now);
        return repository.put(message);
    }

    @Test
    void deletesOldSentMessagesInBatchesButNeverDeadOnes() {
        LocalDateTime old = LocalDateTime.now().minus(8, ChronoUnit.DAYS);
        seedSent("old1", old);
        seedSent("old2", old.plusSeconds(1));
        seedSent("old3", old.plusSeconds(2));
        seedSent("old4", old.plusSeconds(3));
        seedSent("old5", old.plusSeconds(4));
        seedSent("recent", LocalDateTime.now());

        OutboxMessage dead = seedSent("dead", old);
        dead.setStatus(OutboxStatus.DEAD);
        repository.put(dead);

        cleaner.clean();

        assertThat(repository.all()).hasSize(3);
        assertThat(repository.all()).extracting(OutboxMessage::getMsgId)
                .containsExactlyInAnyOrder("old5", "recent", "dead");
    }

    @Test
    void secondRunCleansRemainingBatches() {
        LocalDateTime old = LocalDateTime.now().minus(8, ChronoUnit.DAYS);
        seedSent("old1", old);
        seedSent("old2", old.plusSeconds(1));
        seedSent("old3", old.plusSeconds(2));
        seedSent("old4", old.plusSeconds(3));
        seedSent("old5", old.plusSeconds(4));

        cleaner.clean();
        assertThat(repository.all()).hasSize(1);

        cleaner.clean();
        assertThat(repository.all()).isEmpty();
    }

    @Test
    void singleShardCleanEqualsFullClean() {
        LocalDateTime old = LocalDateTime.now().minus(8, ChronoUnit.DAYS);
        seedSent("old1", old);
        seedSent("old2", old.plusSeconds(1));

        cleaner.clean(0, 1);

        assertThat(repository.all()).isEmpty();
    }

    @Test
    void shardedCleanConvergesAcrossRoundsAndKeepsRecentAndDead() {
        properties.getClean().setBatchSize(10);
        properties.getClean().setMaxBatchesPerRun(10);
        cleaner = new OutboxCleaner(repository, properties);

        LocalDateTime old = LocalDateTime.now().minus(10, ChronoUnit.DAYS);
        LocalDateTime[] times = {
                old,
                old.plus(12, ChronoUnit.HOURS),
                old.plus(1, ChronoUnit.DAYS),
                old.plus(36, ChronoUnit.HOURS),
                old.plus(2, ChronoUnit.DAYS),
                old.plus(60, ChronoUnit.HOURS)
        };
        for (int i = 0; i < times.length; i++) {
            seedSent("old" + i, times[i]);
        }
        seedSent("recent", LocalDateTime.now());
        OutboxMessage dead = seedSent("dead", old);
        dead.setStatus(OutboxStatus.DEAD);
        repository.put(dead);

        // Sequential shard calls let minSent advance between shards, so cleanup converges over
        // rounds rather than completing in a single pass. The lowest slice always deletes the
        // global oldest row, guaranteeing progress; 10 rounds is ample for 6 expired rows.
        for (int round = 0; round < 10; round++) {
            for (int shard = 0; shard < 3; shard++) {
                cleaner.clean(shard, 3);
            }
        }

        assertThat(repository.all()).extracting(OutboxMessage::getMsgId)
                .containsExactlyInAnyOrder("recent", "dead");
    }

    @Test
    void rejectsInvalidShardArguments() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> cleaner.clean(0, 0))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> cleaner.clean(-1, 3))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> cleaner.clean(3, 3))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
