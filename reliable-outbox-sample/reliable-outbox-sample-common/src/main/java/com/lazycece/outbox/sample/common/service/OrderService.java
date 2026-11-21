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
package com.lazycece.outbox.sample.common.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.lazycece.outbox.core.client.MessageClient;
import com.lazycece.outbox.core.model.OutboxMessage;
import com.lazycece.outbox.sample.common.model.Order;
import com.lazycece.outbox.sample.common.repository.OrderRepository;
import java.time.LocalDateTime;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The core demonstration: the order row and the outbox row are written in one local
 * transaction, and the message is published only after that transaction commits.
 *
 * @author lazycece
 */
@Service
public class OrderService {

    public static final String TOPIC = "order-events";

    private static final ObjectMapper JSON = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final OrderRepository orderRepository;
    private final MessageClient messageClient;

    public OrderService(OrderRepository orderRepository, MessageClient messageClient) {
        this.orderRepository = orderRepository;
        this.messageClient = messageClient;
    }

    @Transactional
    public Order placeOrder(PlaceOrderRequest request) {
        Order order = new Order(newId(), request.productId(), request.quantity(),
                request.amount(), LocalDateTime.now());
        orderRepository.insert(order);

        OutboxMessage message = new OutboxMessage();
        message.setBizCode("order");
        message.setTopic(TOPIC);
        message.setPartitionKey(order.id());
        message.setPayload(toJson(order));
        message.setHeaders("{\"source\":\"reliable-outbox-sample\"}");
        messageClient.sendInTransaction(message);

        return order;
    }

    private String toJson(Order order) {
        try {
            return JSON.writeValueAsString(order);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("failed to serialize order", ex);
        }
    }

    private static String newId() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}
