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
package com.lazycece.outbox.dispatch;

import static org.assertj.core.api.Assertions.assertThat;

import com.lazycece.outbox.core.dispatch.RetryPolicy;
import java.time.Duration;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;

/**
 * @author lazycece
 */
class RetryPolicyTest {

    @Test
    void marksDeadAtMaxRetryCount() {
        RetryPolicy policy = new RetryPolicy(16, Duration.ofSeconds(5),
                Duration.ofSeconds(300), Duration.ZERO);
        assertThat(policy.isDead(15)).isFalse();
        assertThat(policy.isDead(16)).isTrue();
    }

    @Test
    void backsOffExponentiallyAndCapsAtMaxInterval() {
        RetryPolicy policy = new RetryPolicy(16, Duration.ofSeconds(5),
                Duration.ofSeconds(300), Duration.ZERO);

        assertThat(delayMillis(policy.nextRetryTime(0))).isBetween(4900L, 5100L);
        assertThat(delayMillis(policy.nextRetryTime(1))).isBetween(9900L, 10100L);
        assertThat(delayMillis(policy.nextRetryTime(6))).isBetween(299900L, 300100L);
        assertThat(delayMillis(policy.nextRetryTime(16))).isBetween(299900L, 300100L);
    }

    @Test
    void zeroJitterDoesNotThrow() {
        RetryPolicy policy = new RetryPolicy(16, Duration.ofSeconds(5),
                Duration.ofSeconds(300), Duration.ZERO);
        assertThat(policy.nextRetryTime(1)).isAfter(LocalDateTime.now());
    }

    @Test
    void jitterStaysWithinConfiguredBound() {
        RetryPolicy policy = new RetryPolicy(16, Duration.ofSeconds(5),
                Duration.ofSeconds(300), Duration.ofMillis(1000));
        long delay = delayMillis(policy.nextRetryTime(0));
        assertThat(delay).isBetween(5000L, 6000L);
    }

    private static long delayMillis(LocalDateTime next) {
        return Duration.between(LocalDateTime.now(), next).toMillis();
    }
}
