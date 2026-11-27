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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.lazycece.outbox.core.model.OutboxMessage;
import com.lazycece.outbox.core.spi.NonRetryablePublishException;
import com.lazycece.outbox.core.spi.PublishException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class RocketMQMessagePublisherTest {

    private final DefaultMQProducer producer = mock(DefaultMQProducer.class);

    @Test
    void publishesMessageWithKeyAndHeaders() throws Exception {
        when(producer.send(any(Message.class), anyLong())).thenReturn(result(SendStatus.SEND_OK));

        publisher().publish(message("m-1", "order-events", "{\"id\":1}",
                "{\"traceId\":\"abc\"}"));

        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
        verify(producer).send(captor.capture(), eq(10_000L));
        Message sent = captor.getValue();
        assertThat(sent.getTopic()).isEqualTo("order-events");
        assertThat(sent.getKeys()).isEqualTo("m-1");
        assertThat(new String(sent.getBody(), StandardCharsets.UTF_8)).isEqualTo("{\"id\":1}");
        assertThat(sent.getProperty("traceId")).isEqualTo("abc");
        assertThat(sent.getProperty(RocketMQHeaders.MSG_ID_PROPERTY)).isEqualTo("m-1");
    }

    @Test
    void malformedHeadersFailWithoutSending() {
        assertThatThrownBy(() -> publisher().publish(
                message("m-2", "order-events", "{}", "{oops")))
                .isInstanceOf(NonRetryablePublishException.class);

        verifyNoInteractions(producer);
    }

    @Test
    void nonOkStatusIsRetryable() throws Exception {
        when(producer.send(any(Message.class), anyLong()))
                .thenReturn(result(SendStatus.FLUSH_DISK_TIMEOUT));

        assertThatThrownBy(() -> publisher().publish(
                message("m-3", "order-events", "{}", null)))
                .isInstanceOf(PublishException.class)
                .isNotInstanceOf(NonRetryablePublishException.class)
                .hasMessageContaining("not acknowledged");
    }

    @Test
    void messageIllegalIsNonRetryable() throws Exception {
        when(producer.send(any(Message.class), anyLong()))
                .thenThrow(new MQBrokerException(ResponseCode.MESSAGE_ILLEGAL, "illegal"));

        assertThatThrownBy(() -> publisher().publish(
                message("m-4", "order-events", "{}", null)))
                .isInstanceOf(NonRetryablePublishException.class)
                .hasCauseInstanceOf(MQBrokerException.class);
    }

    @Test
    void serviceNotAvailableIsRetryable() throws Exception {
        when(producer.send(any(Message.class), anyLong()))
                .thenThrow(new MQBrokerException(ResponseCode.SERVICE_NOT_AVAILABLE, "busy"));

        assertThatThrownBy(() -> publisher().publish(
                message("m-5", "order-events", "{}", null)))
                .isInstanceOf(PublishException.class)
                .isNotInstanceOf(NonRetryablePublishException.class)
                .hasCauseInstanceOf(MQBrokerException.class);
    }

    @Test
    void remotingFailureIsRetryable() throws Exception {
        when(producer.send(any(Message.class), anyLong()))
                .thenThrow(new RemotingException("broker down"));

        assertThatThrownBy(() -> publisher().publish(
                message("m-6", "order-events", "{}", null)))
                .isInstanceOf(PublishException.class)
                .isNotInstanceOf(NonRetryablePublishException.class)
                .hasCauseInstanceOf(RemotingException.class);
    }

    @Test
    void clientFailureIsRetryable() throws Exception {
        when(producer.send(any(Message.class), anyLong()))
                .thenThrow(new MQClientException("no route info of this topic", null));

        assertThatThrownBy(() -> publisher().publish(
                message("m-7", "order-events", "{}", null)))
                .isInstanceOf(PublishException.class)
                .isNotInstanceOf(NonRetryablePublishException.class)
                .hasCauseInstanceOf(MQClientException.class);
    }

    @Test
    void interruptIsRestoredAndWrapped() throws Exception {
        when(producer.send(any(Message.class), anyLong()))
                .thenThrow(new InterruptedException("stop"));
        try {
            assertThatThrownBy(() -> publisher().publish(
                    message("m-8", "order-events", "{}", null)))
                    .isInstanceOf(PublishException.class)
                    .hasMessageContaining("interrupted");
            assertThat(Thread.interrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void unexpectedRuntimeFailureIsRetryable() throws Exception {
        when(producer.send(any(Message.class), anyLong()))
                .thenThrow(new IllegalStateException("boom"));

        assertThatThrownBy(() -> publisher().publish(
                message("m-9", "order-events", "{}", null)))
                .isInstanceOf(PublishException.class)
                .isNotInstanceOf(NonRetryablePublishException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    private RocketMQMessagePublisher publisher() {
        return new RocketMQMessagePublisher(producer, Duration.ofSeconds(10));
    }

    private static SendResult result(SendStatus status) {
        SendResult result = new SendResult();
        result.setSendStatus(status);
        result.setMsgId("broker-msg-id");
        return result;
    }

    private static OutboxMessage message(String msgId, String topic, String payload,
            String headers) {
        OutboxMessage message = new OutboxMessage();
        message.setMsgId(msgId);
        message.setTopic(topic);
        message.setPayload(payload);
        message.setHeaders(headers);
        return message;
    }
}
