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
package com.lazycece.outbox.spi;

import static org.assertj.core.api.Assertions.assertThat;

import com.lazycece.outbox.core.spi.NonRetryablePublishException;
import com.lazycece.outbox.core.spi.PublishException;
import org.junit.jupiter.api.Test;

/**
 * @author lazycece
 * @date 2026/11/11
 */
class PublishExceptionTest {

    @Test
    void nonRetryableIsAPublishException() {
        PublishException retryable = new PublishException("retry", null);
        PublishException nonRetryable = new NonRetryablePublishException("bad topic", null);
        assertThat(nonRetryable).isInstanceOf(PublishException.class);
        assertThat(retryable).isNotInstanceOf(NonRetryablePublishException.class);
        assertThat(nonRetryable.getMessage()).isEqualTo("bad topic");
    }
}
