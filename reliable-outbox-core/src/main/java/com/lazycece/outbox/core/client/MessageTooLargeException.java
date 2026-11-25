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

/**
 * @author lazycece
 */
public class MessageTooLargeException extends IllegalArgumentException {

    private final String field;
    private final int actualBytes;
    private final int maxBytes;

    public MessageTooLargeException(String field, int actualBytes, int maxBytes) {
        super(field + " exceeds limit: actual=" + actualBytes + " bytes, max=" + maxBytes
                + " bytes");
        this.field = field;
        this.actualBytes = actualBytes;
        this.maxBytes = maxBytes;
    }

    public String getField() {
        return field;
    }

    public int getActualBytes() {
        return actualBytes;
    }

    public int getMaxBytes() {
        return maxBytes;
    }
}
