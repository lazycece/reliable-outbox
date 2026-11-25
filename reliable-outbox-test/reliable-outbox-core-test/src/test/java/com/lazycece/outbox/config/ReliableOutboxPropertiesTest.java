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
package com.lazycece.outbox.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.lazycece.outbox.core.config.ReliableOutboxProperties;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * @author lazycece
 */
class ReliableOutboxPropertiesTest {

    @Test
    void defaultsMatchTheDesign() {
        ReliableOutboxProperties p = new ReliableOutboxProperties();
        assertThat(p.isEnabled()).isTrue();
        assertThat(p.getTableName()).isEqualTo("t_outbox");
        assertThat(p.getDispatch().getThreads()).isEqualTo(8);
        assertThat(p.getDispatch().getQueueCapacity()).isEqualTo(1000);
        assertThat(p.getDispatch().getLeaseDuration()).isEqualTo(Duration.ofSeconds(30));
        assertThat(p.getDispatch().getInitialGrace()).isEqualTo(Duration.ofSeconds(10));
        assertThat(p.getScan().getBatchSize()).isEqualTo(100);
        assertThat(p.getScan().getMaxBatchesPerRun()).isEqualTo(10);
        assertThat(p.getRetry().getMaxCount()).isEqualTo(5);
        assertThat(p.getRetry().getInitialInterval()).isEqualTo(Duration.ofSeconds(5));
        assertThat(p.getRetry().getMaxInterval()).isEqualTo(Duration.ofSeconds(60));
        assertThat(p.getRetry().getJitter()).isEqualTo(Duration.ofSeconds(1));
        assertThat(p.getClean().getRetention()).isEqualTo(Duration.ofDays(7));
        assertThat(p.getClean().getBatchSize()).isEqualTo(1000);
        assertThat(p.getClean().getMaxBatchesPerRun()).isEqualTo(10);
        assertThat(p.getLimits().getPayloadBytes()).isEqualTo(65535);
        assertThat(p.getLimits().getHeadersBytes()).isEqualTo(1024);
        p.validate();
    }

    @Test
    void dispatchSizesAndDurationsMustBeValid() {
        ReliableOutboxProperties threads = new ReliableOutboxProperties();
        threads.getDispatch().setThreads(0);
        assertThatThrownBy(threads::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("dispatch.threads");

        ReliableOutboxProperties queue = new ReliableOutboxProperties();
        queue.getDispatch().setQueueCapacity(0);
        assertThatThrownBy(queue::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("dispatch.queue-capacity");

        ReliableOutboxProperties grace = new ReliableOutboxProperties();
        grace.getDispatch().setInitialGrace(Duration.ofSeconds(-1));
        assertThatThrownBy(grace::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("initial-grace");
    }

    @Test
    void scanSizesMustBePositive() {
        ReliableOutboxProperties batch = new ReliableOutboxProperties();
        batch.getScan().setBatchSize(0);
        assertThatThrownBy(batch::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("scan.batch-size");

        ReliableOutboxProperties rounds = new ReliableOutboxProperties();
        rounds.getScan().setMaxBatchesPerRun(0);
        assertThatThrownBy(rounds::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("scan.max-batches-per-run");
    }

    @Test
    void retryBoundsAreValidated() {
        ReliableOutboxProperties count = new ReliableOutboxProperties();
        count.getRetry().setMaxCount(0);
        assertThatThrownBy(count::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("retry.max-count");

        ReliableOutboxProperties initial = new ReliableOutboxProperties();
        initial.getRetry().setInitialInterval(Duration.ZERO);
        assertThatThrownBy(initial::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("retry.initial-interval");

        ReliableOutboxProperties max = new ReliableOutboxProperties();
        max.getRetry().setMaxInterval(Duration.ofSeconds(1));
        assertThatThrownBy(max::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("retry.max-interval");

        ReliableOutboxProperties jitter = new ReliableOutboxProperties();
        jitter.getRetry().setJitter(Duration.ofMillis(-1));
        assertThatThrownBy(jitter::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("retry.jitter");
    }

    @Test
    void cleanParametersAreValidated() {
        ReliableOutboxProperties retention = new ReliableOutboxProperties();
        retention.getClean().setRetention(Duration.ZERO);
        assertThatThrownBy(retention::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("clean.retention");

        ReliableOutboxProperties batch = new ReliableOutboxProperties();
        batch.getClean().setBatchSize(0);
        assertThatThrownBy(batch::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("clean.batch-size");

        ReliableOutboxProperties rounds = new ReliableOutboxProperties();
        rounds.getClean().setMaxBatchesPerRun(0);
        assertThatThrownBy(rounds::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("clean.max-batches-per-run");
    }

    @Test
    void payloadLimitMustFitTextColumn() {
        ReliableOutboxProperties p = new ReliableOutboxProperties();
        p.getLimits().setPayloadBytes(65536);
        assertThatThrownBy(p::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("limits.payload-bytes");

        p.getLimits().setPayloadBytes(0);
        assertThatThrownBy(p::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("limits.payload-bytes");
    }

    @Test
    void headersLimitMustFitVarcharColumn() {
        ReliableOutboxProperties p = new ReliableOutboxProperties();
        p.getLimits().setHeadersBytes(1025);
        assertThatThrownBy(p::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("limits.headers-bytes");

        p.getLimits().setHeadersBytes(0);
        assertThatThrownBy(p::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("limits.headers-bytes");
    }
}
