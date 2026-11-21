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
package com.lazycece.outbox.sample.common.schedule;

import com.lazycece.outbox.core.clean.OutboxCleaner;
import com.lazycece.outbox.core.scan.MessageScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * reliable-outbox does not embed scheduling. The integrator drives the scanner and the
 * cleaner from its own scheduling framework; this demo uses Spring {@code @Scheduled}.
 *
 * @author lazycece
 */
@Component
public class OutboxJobs {

    private static final Logger log = LoggerFactory.getLogger(OutboxJobs.class);

    private final MessageScanner messageScanner;
    private final OutboxCleaner outboxCleaner;

    public OutboxJobs(MessageScanner messageScanner, OutboxCleaner outboxCleaner) {
        this.messageScanner = messageScanner;
        this.outboxCleaner = outboxCleaner;
    }

    @Scheduled(fixedDelayString = "${sample.outbox.scan-interval-ms:1000}")
    public void scan() {
        try {
            messageScanner.scan();
        } catch (Exception ex) {
            log.error("outbox scan failed", ex);
        }
    }

    @Scheduled(
            fixedDelayString = "${sample.outbox.clean-interval-ms:600000}",
            initialDelayString = "${sample.outbox.clean-interval-ms:600000}")
    public void clean() {
        try {
            outboxCleaner.clean();
        } catch (Exception ex) {
            log.error("outbox clean failed", ex);
        }
    }
}
