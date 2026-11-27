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
package com.lazycece.outbox.core.repository;

/**
 * Thrown when the msg_id of an outbox message already exists. The component does not
 * implement idempotent handling; callers decide whether to treat a duplicate as an
 * error, ignore it, or reconcile it.
 *
 * @author lazycece
 */
public class DuplicateMessageException extends RuntimeException {

    private final String msgId;

    public DuplicateMessageException(String msgId) {
        super("duplicate msg_id: " + msgId);
        this.msgId = msgId;
    }

    public DuplicateMessageException(String msgId, Throwable cause) {
        super("duplicate msg_id: " + msgId, cause);
        this.msgId = msgId;
    }

    public String getMsgId() {
        return msgId;
    }
}
