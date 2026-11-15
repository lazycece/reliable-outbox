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

import com.lazycece.outbox.config.ReliableOutboxProperties;
import com.lazycece.outbox.model.OutboxMessage;
import com.lazycece.outbox.model.OutboxStatus;
import com.lazycece.outbox.support.InMemoryOutboxRepository;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author lazycece
 * @date 2026/11/11
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
}
