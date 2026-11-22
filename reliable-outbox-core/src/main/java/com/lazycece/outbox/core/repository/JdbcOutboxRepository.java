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

import com.lazycece.outbox.core.model.OutboxMessage;
import com.lazycece.outbox.core.model.OutboxStatus;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

/**
 * @author lazycece
 * @date 2026/11/11
 */
public class JdbcOutboxRepository implements OutboxRepository {

    private static final Pattern TABLE_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,63}");

    private static final int STATUS_PENDING = OutboxStatus.PENDING.getCode();
    private static final int STATUS_SENT = OutboxStatus.SENT.getCode();
    private static final int STATUS_DEAD = OutboxStatus.DEAD.getCode();

    private final JdbcTemplate jdbcTemplate;
    private final String tableName;

    public JdbcOutboxRepository(JdbcTemplate jdbcTemplate, String tableName) {
        if (tableName == null || !TABLE_NAME.matcher(tableName).matches()) {
            throw new IllegalArgumentException(
                    "Illegal table name, must match [A-Za-z_][A-Za-z0-9_]{0,63}: " + tableName);
        }
        this.jdbcTemplate = jdbcTemplate;
        this.tableName = tableName;
    }

    @Override
    public void insert(OutboxMessage message) {
        try {
            message.setId(doInsert(message));
        } catch (DuplicateKeyException e) {
            // MySQL/InnoDB only: a failed single statement does not abort the transaction.
            throw new DuplicateMessageException(message.getMsgId(), e);
        }
    }

    private long doInsert(OutboxMessage message) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement ps = connection.prepareStatement(
                    sql(OutboxSql.INSERT), Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, message.getMsgId());
            ps.setString(2, message.getBizCode());
            ps.setString(3, message.getTopic());
            ps.setString(4, message.getPartitionKey());
            ps.setString(5, message.getPayload());
            ps.setString(6, message.getHeaders());
            ps.setInt(7, STATUS_PENDING);
            ps.setObject(8, message.getNextRetryTime());
            ps.setObject(9, message.getCreatedAt());
            ps.setObject(10, message.getUpdatedAt());
            return ps;
        }, keyHolder);
        Number key = keyHolder.getKey();
        if (key == null) {
            throw new IllegalStateException(
                    "No generated key returned after inserting outbox message");
        }
        return key.longValue();
    }

    @Override
    public OutboxMessage findById(Long id) {
        List<OutboxMessage> list = jdbcTemplate.query(
                sql(OutboxSql.FIND_BY_ID), outboxRowMapper(), id);
        return list.isEmpty() ? null : list.get(0);
    }

    @Override
    public OutboxMessage findByMsgId(String msgId) {
        List<OutboxMessage> list = jdbcTemplate.query(
                sql(OutboxSql.FIND_BY_MSG_ID), outboxRowMapper(), msgId);
        return list.isEmpty() ? null : list.get(0);
    }

    private RowMapper<OutboxMessage> outboxRowMapper() {
        return (rs, rowNum) -> {
            OutboxMessage message = new OutboxMessage();
            message.setId(rs.getLong("id"));
            message.setMsgId(rs.getString("msg_id"));
            message.setBizCode(rs.getString("biz_code"));
            message.setTopic(rs.getString("topic"));
            message.setPartitionKey(rs.getString("partition_key"));
            message.setPayload(rs.getString("payload"));
            message.setHeaders(rs.getString("headers"));
            message.setStatus(OutboxStatus.fromCode(rs.getInt("status")));
            message.setRetryCount(rs.getInt("retry_count"));
            message.setNextRetryTime(readTime(rs, "next_retry_time"));
            message.setOwnerToken(rs.getString("owner_token"));
            message.setOwnerExpireAt(readTime(rs, "owner_expire_at"));
            message.setSentAt(readTime(rs, "sent_at"));
            message.setLastError(rs.getString("last_error"));
            message.setCreatedAt(readTime(rs, "created_at"));
            message.setUpdatedAt(readTime(rs, "updated_at"));
            return message;
        };
    }

    private static LocalDateTime readTime(ResultSet rs, String column) throws SQLException {
        return rs.getObject(column, LocalDateTime.class);
    }

    @Override
    public boolean tryAcquire(Long id, String ownerToken, LocalDateTime ownerExpireAt,
            LocalDateTime now) {
        int rows = jdbcTemplate.update(
                sql(OutboxSql.TRY_ACQUIRE),
                ownerToken, ownerExpireAt, now, id, STATUS_PENDING, now);
        return rows == 1;
    }

    @Override
    public boolean tryAcquireDue(Long id, String ownerToken, LocalDateTime ownerExpireAt,
            LocalDateTime dueAt) {
        int rows = jdbcTemplate.update(
                sql(OutboxSql.TRY_ACQUIRE_DUE),
                ownerToken, ownerExpireAt, dueAt, id, STATUS_PENDING, dueAt, dueAt);
        return rows == 1;
    }

    @Override
    public boolean releaseLease(Long id, String ownerToken, LocalDateTime now) {
        int rows = jdbcTemplate.update(
                sql(OutboxSql.RELEASE_LEASE), now, id, ownerToken);
        return rows == 1;
    }

    @Override
    public boolean markSent(Long id, String ownerToken, LocalDateTime sentAt,
            int retryCount) {
        int rows = jdbcTemplate.update(
                sql(OutboxSql.MARK_SENT),
                STATUS_SENT, sentAt, retryCount, sentAt, id, ownerToken, STATUS_PENDING);
        return rows == 1;
    }

    @Override
    public boolean markFailed(Long id, String ownerToken, String error,
            LocalDateTime nextRetryTime, int retryCount,
            boolean dead, LocalDateTime now) {
        if (dead) {
            int rows = jdbcTemplate.update(
                    sql(OutboxSql.MARK_FAILED_DEAD),
                    STATUS_DEAD, retryCount, error, now, id, ownerToken, STATUS_PENDING);
            return rows == 1;
        }
        int rows = jdbcTemplate.update(
                sql(OutboxSql.MARK_FAILED_RETRY),
                STATUS_PENDING, retryCount, nextRetryTime, error, now, id, ownerToken,
                STATUS_PENDING);
        return rows == 1;
    }

    @Override
    public boolean resetForReplay(String msgId, LocalDateTime nextRetryTime, LocalDateTime now) {
        int rows = jdbcTemplate.update(
                sql(OutboxSql.RESET_FOR_REPLAY),
                STATUS_PENDING, nextRetryTime, now, msgId, STATUS_DEAD);
        return rows == 1;
    }

    @Override
    public List<Long> scanDispatchableIds(int limit, LocalDateTime now) {
        return jdbcTemplate.queryForList(
                sql(OutboxSql.SCAN_DISPATCHABLE),
                Long.class, STATUS_PENDING, now, now, limit);
    }

    @Override
    public LocalDateTime minSentAt() {
        List<LocalDateTime> result = jdbcTemplate.queryForList(
                sql(OutboxSql.MIN_SENT_AT), LocalDateTime.class, STATUS_SENT);
        return result.isEmpty() ? null : result.get(0);
    }

    @Override
    public List<Long> scanSentBetween(LocalDateTime from, LocalDateTime to, int limit) {
        return jdbcTemplate.queryForList(
                sql(OutboxSql.SCAN_SENT_BETWEEN),
                Long.class, STATUS_SENT, from, to, limit);
    }

    @Override
    public int deleteSent(List<Long> ids) {
        if (ids.isEmpty()) {
            return 0;
        }
        String inSql = String.join(",", Collections.nCopies(ids.size(), "?"));
        Object[] params = ids.toArray(new Object[ids.size() + 1]);
        params[ids.size()] = STATUS_SENT;
        return jdbcTemplate.update(
                OutboxSql.DELETE_SENT.formatted(tableName, inSql), params);
    }

    private String sql(String template) {
        return template.formatted(tableName);
    }
}
