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
package com.lazycece.outbox.core.spi;

import com.lazycece.outbox.core.model.OutboxMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lazycece
 */
public class LoggingMessageListener implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(LoggingMessageListener.class);

    @Override
    public void onSent(OutboxMessage message) {
        log.debug("outbox message sent, msgId={}", message.getMsgId());
    }

    @Override
    public void onFailed(OutboxMessage message, int retryCount, Throwable error) {
        log.warn("outbox message failed, msgId={}, retryCount={}", message.getMsgId(), retryCount,
                error);
    }

    @Override
    public void onDead(OutboxMessage message) {
        log.error(
                "outbox message DEAD, msgId={}, bizCode={}, topic={}, retryCount={}, lastError={}",
                message.getMsgId(), message.getBizCode(), message.getTopic(),
                message.getRetryCount(), message.getLastError());
    }

    @Override
    public void onLeaseLost(OutboxMessage message, String reason) {
        log.warn("outbox lease lost, msgId={}, reason={}", message.getMsgId(), reason);
    }
}
