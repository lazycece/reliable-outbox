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

/**
 * @author lazycece
 * @date 2026/11/11
 */
public enum OutboxStatus {
    PENDING(0), SENT(1), DEAD(2);

    private final int code;

    OutboxStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static OutboxStatus fromCode(int code) {
        for (OutboxStatus status : values()) {
            if (status.code == code) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown outbox status code: " + code);
    }

    public boolean isDispatchable() {
        return this == PENDING;
    }
}
