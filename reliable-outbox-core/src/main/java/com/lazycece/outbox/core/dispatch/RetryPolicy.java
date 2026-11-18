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
package com.lazycece.outbox.core.dispatch;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Retry policy.
 * <p>
 * {@code maxRetryCount} is the number of allowed retries, <b>excluding the first delivery</b>.
 * Every failed retry increments the number of performed retries; once the limit is reached the
 * message moves to DEAD. A failed first delivery only schedules retry #1 and does not consume
 * the retry budget.
 *
 * @author lazycece
 * @date 2026/11/11
 */
public class RetryPolicy {

    private final int maxRetryCount;
    private final Duration initialInterval;
    private final Duration maxInterval;
    private final long maxJitterMillis;

    public RetryPolicy(int maxRetryCount, Duration initialInterval,
            Duration maxInterval, Duration maxJitter) {
        this.maxRetryCount = maxRetryCount;
        this.initialInterval = initialInterval;
        this.maxInterval = maxInterval;
        this.maxJitterMillis = maxJitter.toMillis();
    }

    /**
     * @param retriesPerformed number of retries already performed (the first delivery does not
     *                         count)
     */
    public boolean isDead(int retriesPerformed) {
        return retriesPerformed >= maxRetryCount;
    }

    /**
     * Computes the time of the next retry. After the k-th retry fails, the wait is
     * {@code min(initialInterval * 2^k, maxInterval)} (k is the number of retries performed).
     *
     * @param retriesPerformed number of retries already performed (the first delivery does not
     *                         count)
     */
    public LocalDateTime nextRetryTime(int retriesPerformed) {
        int shift = Math.min(Math.max(0, retriesPerformed), 20);
        long baseMillis = Math.min(initialInterval.toMillis() * (1L << shift),
                maxInterval.toMillis());
        long jitterMillis = maxJitterMillis > 0
                ? ThreadLocalRandom.current().nextLong(maxJitterMillis)
                : 0L;
        return LocalDateTime.now().plus(Duration.ofMillis(baseMillis + jitterMillis));
    }
}
