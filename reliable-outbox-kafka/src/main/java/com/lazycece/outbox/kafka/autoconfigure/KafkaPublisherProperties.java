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
package com.lazycece.outbox.kafka.autoconfigure;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Kafka publisher adapter settings.
 *
 * @author lazycece
 */
@ConfigurationProperties(prefix = "reliable-outbox.kafka")
public class KafkaPublisherProperties {

    /**
     * Whether the Kafka publisher adapter is enabled. Disable it when another
     * {@code MessagePublisher} adapter is active in the same application.
     */
    private boolean enabled = true;

    /**
     * How long to wait for the broker acknowledgement before failing the publish.
     */
    private Duration sendTimeout = Duration.ofSeconds(5);

    @PostConstruct
    void validate() {
        if (sendTimeout == null || sendTimeout.isZero() || sendTimeout.isNegative()) {
            throw new IllegalStateException(
                    "reliable-outbox.kafka.send-timeout must be greater than 0");
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Duration getSendTimeout() {
        return sendTimeout;
    }

    public void setSendTimeout(Duration sendTimeout) {
        this.sendTimeout = sendTimeout;
    }
}
