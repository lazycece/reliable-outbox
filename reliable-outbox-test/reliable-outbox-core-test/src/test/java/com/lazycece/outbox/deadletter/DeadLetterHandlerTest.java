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
package com.lazycece.outbox.deadletter;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.lazycece.outbox.core.deadletter.DeadLetterHandler;
import com.lazycece.outbox.core.model.OutboxMessage;
import com.lazycece.outbox.core.spi.MessageListener;
import org.junit.jupiter.api.Test;

/**
 * @author lazycece
 */
class DeadLetterHandlerTest {

    @Test
    void listenerFailuresAreSwallowed() {
        MessageListener listener = mock(MessageListener.class);
        doThrow(new RuntimeException("listener down")).when(listener).onDead(any());
        DeadLetterHandler handler = new DeadLetterHandler(listener);

        assertThatCode(() -> handler.onDead(new OutboxMessage())).doesNotThrowAnyException();
        verify(listener).onDead(any(OutboxMessage.class));
    }
}
