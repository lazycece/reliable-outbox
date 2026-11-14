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

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ThreadLocalRandom;

/**
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

    public boolean isDead(int retryCount) {
        return retryCount >= maxRetryCount;
    }

    public LocalDateTime nextRetryTime(int retryCount) {
        int shift = Math.min(Math.max(0, retryCount - 1), 20);
        long baseMillis = Math.min(initialInterval.toMillis() * (1L << shift),
                maxInterval.toMillis());
        long jitterMillis = maxJitterMillis > 0
                ? ThreadLocalRandom.current().nextLong(maxJitterMillis)
                : 0L;
        return LocalDateTime.now().plus(Duration.ofMillis(baseMillis + jitterMillis));
    }
}
