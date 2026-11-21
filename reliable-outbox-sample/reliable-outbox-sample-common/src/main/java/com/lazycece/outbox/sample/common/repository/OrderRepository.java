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
package com.lazycece.outbox.sample.common.repository;

import com.lazycece.outbox.sample.common.model.Order;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Plain JDBC persistence for the demo order table.
 *
 * @author lazycece
 */
@Repository
public class OrderRepository {

    private final JdbcTemplate jdbcTemplate;

    public OrderRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(Order order) {
        jdbcTemplate.update(
                "INSERT INTO t_order (id, product_id, quantity, amount, created_at) "
                        + "VALUES (?, ?, ?, ?, ?)",
                order.id(), order.productId(), order.quantity(), order.amount(),
                order.createdAt());
    }

    public Optional<Order> findById(String id) {
        List<Order> orders = jdbcTemplate.query(
                "SELECT id, product_id, quantity, amount, created_at "
                        + "FROM t_order WHERE id = ?",
                (rs, rowNum) -> new Order(
                        rs.getString("id"),
                        rs.getString("product_id"),
                        rs.getInt("quantity"),
                        rs.getBigDecimal("amount"),
                        rs.getObject("created_at", java.time.LocalDateTime.class)),
                id);
        return orders.stream().findFirst();
    }
}
