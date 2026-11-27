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
package com.lazycece.outbox.sample.common.web;

import com.lazycece.outbox.core.deadletter.DeadLetterReplayer;
import com.lazycece.outbox.core.model.OutboxStatus;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read-only visibility into the outbox table plus manual dead-letter replay.
 *
 * @author lazycece
 */
@RestController
@RequestMapping("/outbox")
public class OutboxController {

    private static final String SELECT_RECENT = """
            SELECT msg_id, biz_code, topic, partition_key, status, retry_count,
                   last_error, next_retry_time, sent_at, created_at
            FROM t_outbox
            ORDER BY id DESC
            LIMIT 50
            """;

    private final JdbcTemplate jdbcTemplate;
    private final DeadLetterReplayer deadLetterReplayer;

    public OutboxController(JdbcTemplate jdbcTemplate, DeadLetterReplayer deadLetterReplayer) {
        this.jdbcTemplate = jdbcTemplate;
        this.deadLetterReplayer = deadLetterReplayer;
    }

    @GetMapping
    public List<OutboxView> recent() {
        return jdbcTemplate.query(SELECT_RECENT, (rs, rowNum) -> new OutboxView(
                rs.getString("msg_id"),
                rs.getString("biz_code"),
                rs.getString("topic"),
                rs.getString("partition_key"),
                OutboxStatus.fromCode(rs.getInt("status")).name(),
                rs.getInt("retry_count"),
                rs.getString("last_error"),
                rs.getObject("next_retry_time", LocalDateTime.class),
                rs.getObject("sent_at", LocalDateTime.class),
                rs.getObject("created_at", LocalDateTime.class)));
    }

    @PostMapping("/{msgId}/replay")
    public ResponseEntity<Map<String, Object>> replay(@PathVariable("msgId") String msgId) {
        boolean replayed = deadLetterReplayer.replay(msgId);
        if (replayed) {
            return ResponseEntity.ok(Map.of("msgId", msgId, "replayed", true));
        }
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of("msgId", msgId, "replayed", false,
                        "reason", "message is not in DEAD status or does not exist"));
    }

    /**
     * Flat view of one outbox row for easier inspection from curl / jq.
     */
    public record OutboxView(
            String msgId,
            String bizCode,
            String topic,
            String partitionKey,
            String status,
            int retryCount,
            String lastError,
            LocalDateTime nextRetryTime,
            LocalDateTime sentAt,
            LocalDateTime createdAt) {
    }
}
