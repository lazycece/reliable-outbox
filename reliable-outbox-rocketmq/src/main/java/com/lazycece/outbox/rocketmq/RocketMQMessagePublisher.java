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
package com.lazycece.outbox.rocketmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lazycece.outbox.core.model.OutboxMessage;
import com.lazycece.outbox.core.spi.MessagePublisher;
import com.lazycece.outbox.core.spi.NonRetryablePublishException;
import com.lazycece.outbox.core.spi.PublishException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * Publishes outbox messages to RocketMQ and waits for the broker acknowledgement.
 *
 * <p>The outbox {@code msgId} is used as the RocketMQ message key (for console lookup) and
 * forwarded as the reserved {@code outbox-msg-id} user property. RocketMQ's own message id is
 * generated later by the broker and is not used for idempotency.
 *
 * @author lazycece
 */
public final class RocketMQMessagePublisher implements MessagePublisher {

    private final DefaultMQProducer producer;
    private final Duration sendTimeout;
    private final RocketMQHeaders headers = new RocketMQHeaders(new ObjectMapper());

    public RocketMQMessagePublisher(DefaultMQProducer producer, Duration sendTimeout) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.sendTimeout = Objects.requireNonNull(sendTimeout, "sendTimeout");
    }

    @Override
    public void publish(OutboxMessage message) {
        Message rocketMessage = new Message(message.getTopic(), null, message.getMsgId(),
                message.getPayload().getBytes(StandardCharsets.UTF_8));
        headers.apply(message, rocketMessage);
        try {
            SendResult result = producer.send(rocketMessage, sendTimeout.toMillis());
            if (result == null || result.getSendStatus() != SendStatus.SEND_OK) {
                throw new PublishException("RocketMQ send not acknowledged: "
                        + (result == null ? "null result" : result.getSendStatus()), null);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new PublishException("RocketMQ publish interrupted", ex);
        } catch (MQClientException | MQBrokerException | RemotingException ex) {
            throw translate(ex);
        } catch (PublishException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            throw translate(ex);
        }
    }

    private PublishException translate(Exception cause) {
        if (cause instanceof MQBrokerException broker
                && broker.getResponseCode() == ResponseCode.MESSAGE_ILLEGAL) {
            return new NonRetryablePublishException(
                    "RocketMQ publish failed permanently: " + cause.getMessage(), cause);
        }
        return new PublishException("RocketMQ publish failed: " + cause.getMessage(), cause);
    }
}
