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

/**
 * SQL statement templates used by {@link JdbcOutboxRepository}.
 *
 * <p>Each template contains a {@code %s} placeholder for the outbox table name.
 *
 * @author lazycece
 */
final class OutboxSql {

    private OutboxSql() {
    }

    static final String COLUMNS = """
            id, msg_id, biz_code, topic, partition_key, payload, headers, status, retry_count, \
            next_retry_time, owner_token, owner_expire_at, sent_at, last_error, created_at, \
            updated_at""";

    static final String INSERT = """
            INSERT INTO %s
                (msg_id, biz_code, topic, partition_key, payload, headers,
                 status, retry_count, next_retry_time, created_at, updated_at)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)""";

    static final String FIND_BY_ID =
            "SELECT " + COLUMNS + " FROM %s WHERE id = ?";

    static final String FIND_BY_MSG_ID =
            "SELECT " + COLUMNS + " FROM %s WHERE msg_id = ?";

    static final String TRY_ACQUIRE = """
            UPDATE %s
               SET owner_token = ?, owner_expire_at = ?, updated_at = ?
             WHERE id = ?
               AND status = ?
               AND (owner_token IS NULL OR owner_expire_at < ?)""";

    static final String TRY_ACQUIRE_DUE = """
            UPDATE %s
               SET owner_token = ?, owner_expire_at = ?, updated_at = ?
             WHERE id = ?
               AND status = ?
               AND next_retry_time IS NOT NULL
               AND next_retry_time <= ?
               AND (owner_token IS NULL OR owner_expire_at < ?)""";

    static final String RELEASE_LEASE = """
            UPDATE %s
               SET owner_token = NULL, owner_expire_at = NULL, updated_at = ?
             WHERE id = ?
               AND owner_token = ?""";

    static final String MARK_SENT = """
            UPDATE %s
               SET status = ?,
                   owner_token = NULL,
                   owner_expire_at = NULL,
                   sent_at = ?,
                   retry_count = ?,
                   next_retry_time = NULL,
                   last_error = NULL,
                   updated_at = ?
             WHERE id = ?
               AND owner_token = ?
               AND status = ?""";

    static final String MARK_FAILED_DEAD = """
            UPDATE %s
               SET status = ?,
                   owner_token = NULL,
                   owner_expire_at = NULL,
                   retry_count = ?,
                   next_retry_time = NULL,
                   last_error = ?,
                   updated_at = ?
             WHERE id = ?
               AND owner_token = ?
               AND status = ?""";

    static final String MARK_FAILED_RETRY = """
            UPDATE %s
               SET status = ?,
                   owner_token = NULL,
                   owner_expire_at = NULL,
                   retry_count = ?,
                   next_retry_time = ?,
                   last_error = ?,
                   updated_at = ?
             WHERE id = ?
               AND owner_token = ?
               AND status = ?""";

    static final String RESET_FOR_REPLAY = """
            UPDATE %s
               SET status = ?,
                   retry_count = 0,
                   next_retry_time = ?,
                   owner_token = NULL,
                   owner_expire_at = NULL,
                   last_error = NULL,
                   updated_at = ?
             WHERE msg_id = ?
               AND status = ?""";

    static final String SCAN_DISPATCHABLE = """
            SELECT id
              FROM %s
             WHERE status = ?
               AND next_retry_time IS NOT NULL
               AND next_retry_time <= ?
               AND (owner_token IS NULL OR owner_expire_at < ?)
             ORDER BY next_retry_time, id
             LIMIT ?""";

    static final String MIN_SENT_AT =
            "SELECT MIN(sent_at) FROM %s WHERE status = ?";

    static final String SCAN_SENT_BETWEEN = """
            SELECT id
              FROM %s
             WHERE status = ?
               AND sent_at >= ?
               AND sent_at < ?
             ORDER BY sent_at, id
             LIMIT ?""";

    static final String DELETE_SENT = """
            DELETE FROM %s
             WHERE id IN (%s)
               AND status = ?""";
}
