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

import static org.assertj.core.api.Assertions.assertThat;

import com.lazycece.outbox.core.spi.MessagePublisher;
import com.lazycece.outbox.sample.common.model.Order;
import com.lazycece.outbox.sample.common.repository.OrderRepository;
import com.lazycece.outbox.sample.common.service.OrderService;
import com.lazycece.outbox.sample.common.service.PlaceOrderRequest;
import java.math.BigDecimal;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Verifies the business + outbox wiring without a broker by replacing the Kafka publisher
 * with a stub. The real Kafka path is exercised by the docker-compose demo.
 *
 * @author lazycece
 */
@SpringBootTest(properties = {
        "reliable-outbox.kafka.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "reliable-outbox.dispatch.initial-grace=0s",
        "sample.outbox.scan-interval-ms=200"
})
@Import(SampleApplicationTest.StubPublisherConfig.class)
class SampleApplicationTest {

    @Autowired
    private OrderService orderService;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void placingOrderPersistsOrderAndOutboxMessageThenMarksItSent() throws Exception {
        Order order = orderService.placeOrder(
                new PlaceOrderRequest("sku-1", 2, new BigDecimal("19.90")));

        assertThat(orderRepository.findById(order.id())).isPresent();
        assertThat(awaitSent()).isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT payload FROM t_outbox", String.class)).contains(order.id());
    }

    private int awaitSent() throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        int sent = 0;
        while (System.nanoTime() < deadline) {
            sent = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM t_outbox WHERE status = 1", Integer.class);
            if (sent == 1) {
                return sent;
            }
            Thread.sleep(100);
        }
        return sent;
    }

    @TestConfiguration
    static class StubPublisherConfig {

        @Bean
        MessagePublisher stubMessagePublisher() {
            return message -> {
            };
        }
    }
}
