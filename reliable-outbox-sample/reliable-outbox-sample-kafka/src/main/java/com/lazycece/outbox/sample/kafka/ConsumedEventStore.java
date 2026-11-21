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
package com.lazycece.outbox.sample.kafka;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.springframework.stereotype.Component;

/**
 * In-memory buffer of the latest events consumed from Kafka, exposed for the demo.
 *
 * @author lazycece
 */
@Component
public class ConsumedEventStore {

    private static final int MAX_EVENTS = 100;

    private final Deque<ConsumedEvent> events = new ConcurrentLinkedDeque<>();

    public void add(ConsumedEvent event) {
        events.addFirst(event);
        while (events.size() > MAX_EVENTS) {
            events.pollLast();
        }
    }

    public List<ConsumedEvent> recent() {
        return new ArrayList<>(events);
    }

    public record ConsumedEvent(
            String msgId,
            String partitionKey,
            String payload,
            Instant receivedAt) {
    }
}
