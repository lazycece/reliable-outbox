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
package com.lazycece.outbox.rocketmq.autoconfigure;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * RocketMQ publisher adapter settings.
 *
 * @author lazycece
 */
@ConfigurationProperties(prefix = "reliable-outbox.rocketmq")
public class RocketMQPublisherProperties {

    /**
     * Whether the RocketMQ publisher adapter is enabled. Disable it when another
     * {@code MessagePublisher} adapter is active in the same application.
     */
    private boolean enabled = true;

    /**
     * RocketMQ name server address, e.g. {@code 127.0.0.1:9876}. Required unless the application
     * provides its own {@code DefaultMQProducer} bean.
     */
    private String namesrvAddr;

    /**
     * Producer group used when this adapter creates the {@code DefaultMQProducer}.
     */
    private String producerGroup = "reliable-outbox-producer";

    /**
     * How long to wait for the broker acknowledgement before failing the publish.
     */
    private Duration sendTimeout = Duration.ofSeconds(5);

    @PostConstruct
    void validate() {
        if (sendTimeout == null || sendTimeout.isZero() || sendTimeout.isNegative()) {
            throw new IllegalStateException(
                    "reliable-outbox.rocketmq.send-timeout must be greater than 0");
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public Duration getSendTimeout() {
        return sendTimeout;
    }

    public void setSendTimeout(Duration sendTimeout) {
        this.sendTimeout = sendTimeout;
    }
}
