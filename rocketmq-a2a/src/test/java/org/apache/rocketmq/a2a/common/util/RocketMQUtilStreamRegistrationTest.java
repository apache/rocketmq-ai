/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.a2a.common.util;

import io.a2a.client.transport.spi.interceptors.PayloadAndHeaders;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.RecallReceipt;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.apis.producer.Transaction;
import org.junit.Assert;
import org.junit.Test;

public class RocketMQUtilStreamRegistrationTest {

    @Test
    public void testRequestIdIsExposedBeforeSend() throws Exception {
        AtomicBoolean callbackInvoked = new AtomicBoolean(false);
        Producer producer = new Producer() {
            @Override
            public SendReceipt send(Message message) {
                Assert.assertTrue("stream listener must be registered before send", callbackInvoked.get());
                return receipt();
            }

            @Override
            public SendReceipt send(Message message, Transaction transaction) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CompletableFuture<SendReceipt> sendAsync(Message message) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Transaction beginTransaction() {
                throw new UnsupportedOperationException();
            }

            @Override
            public RecallReceipt recallMessage(String messageId, String recallHandle) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CompletableFuture<RecallReceipt> recallMessageAsync(String messageId, String recallHandle) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() throws IOException {
            }
        };

        String requestId = RocketMQUtil.sendRocketMQRequest(
            new PayloadAndHeaders(Map.of(), Map.of()),
            "agent-topic",
            "lite-topic",
            "response-topic",
            producer,
            null,
            id -> callbackInvoked.set(true)
        );

        Assert.assertTrue(callbackInvoked.get());
        Assert.assertNotNull(requestId);
    }

    private static SendReceipt receipt() {
        return new SendReceipt() {
            @Override
            public MessageId getMessageId() {
                return new MessageId() {
                    @Override
                    public String getVersion() {
                        return "1";
                    }

                    @Override
                    public String toString() {
                        return "test-message-id";
                    }
                };
            }

            @Override
            public String getRecallHandle() {
                return null;
            }
        };
    }
}
