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
package com.lazycece.outbox.core.deadletter;

import com.lazycece.outbox.core.dispatch.MessageDispatcher;
import com.lazycece.outbox.core.model.OutboxMessage;
import com.lazycece.outbox.core.repository.OutboxRepository;
import java.time.LocalDateTime;

/**
 * @author lazycece
 * @date 2026/11/11
 */
public class DeadLetterReplayer {

    private final OutboxRepository repository;
    private final MessageDispatcher dispatcher;

    public DeadLetterReplayer(OutboxRepository repository, MessageDispatcher dispatcher) {
        this.repository = repository;
        this.dispatcher = dispatcher;
    }

    public boolean replay(String msgId) {
        LocalDateTime now = LocalDateTime.now();
        boolean reset = repository.resetForReplay(msgId, now, now);
        if (!reset) {
            return false;
        }
        OutboxMessage message = repository.findByMsgId(msgId);
        if (message != null) {
            dispatcher.dispatchById(message.getId());
        }
        return true;
    }
}
