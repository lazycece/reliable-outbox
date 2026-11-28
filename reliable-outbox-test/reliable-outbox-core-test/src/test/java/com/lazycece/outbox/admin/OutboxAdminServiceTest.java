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
package com.lazycece.outbox.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.lazycece.outbox.core.admin.OutboxAdminRepository;
import com.lazycece.outbox.core.admin.OutboxAdminService;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * @author lazycece
 */
class OutboxAdminServiceTest {

    @Test
    void chunksMsgIdsAndSumsAffectedRows() {
        RecordingRepository repository = new RecordingRepository();
        OutboxAdminService service = new OutboxAdminService(repository, 2);

        int affected = service.replayDead(List.of("a", "b", "c", "d", "e"));

        assertThat(repository.chunkSizes).containsExactly(2, 2, 1);
        assertThat(affected).isEqualTo(5);
    }

    @Test
    void emptyOrNullInputReturnsZeroWithoutCallingRepository() {
        RecordingRepository repository = new RecordingRepository();
        OutboxAdminService service = new OutboxAdminService(repository, 10);

        assertThat(service.replayDead(List.of())).isZero();
        assertThat(service.retryPendingNow(null)).isZero();
        assertThat(service.deleteDead(java.util.Collections.emptyList())).isZero();
        assertThat(repository.calls).isZero();
    }

    @Test
    void blankReasonIsRejected() {
        OutboxAdminService service = new OutboxAdminService(new RecordingRepository(), 10);

        assertThatThrownBy(() -> service.markDead(List.of("a"), "  "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.markDead(List.of("a"), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void chunkSizeMustBePositive() {
        assertThatThrownBy(() -> new OutboxAdminService(new RecordingRepository(), 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    static final class RecordingRepository implements OutboxAdminRepository {

        private final List<Integer> chunkSizes = new ArrayList<>();
        private int calls;

        @Override
        public int replayDead(Collection<String> msgIds, LocalDateTime now) {
            return record(msgIds);
        }

        @Override
        public int retryPendingNow(Collection<String> msgIds, LocalDateTime now) {
            return record(msgIds);
        }

        @Override
        public int markPendingAsDead(Collection<String> msgIds, String reason,
                LocalDateTime now) {
            return record(msgIds);
        }

        @Override
        public int deleteDead(Collection<String> msgIds) {
            return record(msgIds);
        }

        private int record(Collection<String> msgIds) {
            calls++;
            chunkSizes.add(msgIds.size());
            return msgIds.size();
        }
    }
}
