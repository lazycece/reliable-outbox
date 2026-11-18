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
package com.lazycece.outbox.core.client;

import com.lazycece.outbox.core.model.OutboxMessage;
import com.lazycece.outbox.core.repository.DuplicateMessageException;

/**
 * @author lazycece
 * @date 2026/11/11
 */
public interface MessageClient {

    /**
     * Must join an existing business transaction.
     *
     * @throws DuplicateMessageException when msg_id already exists; the surrounding
     *                                   transaction is marked rollback-only
     */
    String sendInTransaction(OutboxMessage message);

    /**
     * Uses an independent REQUIRES_NEW transaction.
     *
     * @throws DuplicateMessageException when msg_id already exists
     */
    String sendStandalone(OutboxMessage message);
}
