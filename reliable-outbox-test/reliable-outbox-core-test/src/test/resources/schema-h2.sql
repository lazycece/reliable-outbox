CREATE TABLE IF NOT EXISTS t_outbox (
    id BIGINT NOT NULL AUTO_INCREMENT,
    msg_id VARCHAR(64) NOT NULL,
    biz_code VARCHAR(64) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    partition_key VARCHAR(255) DEFAULT NULL,
    payload CLOB NOT NULL,
    headers VARCHAR(1024) DEFAULT NULL,
    status TINYINT NOT NULL DEFAULT 0,
    retry_count INT NOT NULL DEFAULT 0,
    next_retry_time TIMESTAMP(3) DEFAULT NULL,
    owner_token VARCHAR(64) DEFAULT NULL,
    owner_expire_at TIMESTAMP(3) DEFAULT NULL,
    sent_at TIMESTAMP(3) DEFAULT NULL,
    last_error VARCHAR(1000) DEFAULT NULL,
    created_at TIMESTAMP(3) NOT NULL,
    updated_at TIMESTAMP(3) NOT NULL ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    CONSTRAINT uk_msg_id UNIQUE (msg_id)
);
CREATE INDEX IF NOT EXISTS idx_status_next_retry ON t_outbox (status, next_retry_time);
CREATE INDEX IF NOT EXISTS idx_status_sent ON t_outbox (status, sent_at);
