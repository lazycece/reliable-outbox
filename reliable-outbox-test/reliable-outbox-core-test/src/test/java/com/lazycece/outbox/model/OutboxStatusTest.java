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
package com.lazycece.outbox.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.lazycece.outbox.core.model.OutboxStatus;
import org.junit.jupiter.api.Test;

/**
 * @author lazycece
 * @date 2026/11/11
 */
class OutboxStatusTest {

    @Test
    void onlyPendingIsDispatchable() {
        assertThat(OutboxStatus.PENDING.isDispatchable()).isTrue();
        assertThat(OutboxStatus.SENT.isDispatchable()).isFalse();
        assertThat(OutboxStatus.DEAD.isDispatchable()).isFalse();
    }

    @Test
    void codesAreStableAndRoundTrip() {
        assertThat(OutboxStatus.PENDING.getCode()).isZero();
        assertThat(OutboxStatus.SENT.getCode()).isEqualTo(1);
        assertThat(OutboxStatus.DEAD.getCode()).isEqualTo(2);
        assertThat(OutboxStatus.fromCode(0)).isEqualTo(OutboxStatus.PENDING);
        assertThat(OutboxStatus.fromCode(1)).isEqualTo(OutboxStatus.SENT);
        assertThat(OutboxStatus.fromCode(2)).isEqualTo(OutboxStatus.DEAD);
        assertThatThrownBy(() -> OutboxStatus.fromCode(99))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
