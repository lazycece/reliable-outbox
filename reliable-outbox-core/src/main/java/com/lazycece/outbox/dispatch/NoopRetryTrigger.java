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

/**
 * Default {@link RetryTrigger} that does nothing. Failed messages are left to the
 * integrator-driven {@code MessageScanner}, which picks them up once
 * {@code next_retry_time} is due. Integrators can provide their own bean to trigger
 * retries without waiting for the next scan round.
 *
 * @author lazycece
 * @date 2026/11/11
 */
public class NoopRetryTrigger implements RetryTrigger {

    @Override
    public void trigger(Long messageId, long delayMillis) {
        // no-op: the scanner is the fallback retry mechanism
    }
}
