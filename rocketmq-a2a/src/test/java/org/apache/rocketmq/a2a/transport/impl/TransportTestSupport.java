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
package org.apache.rocketmq.a2a.transport.impl;

import com.alibaba.fastjson.JSON;
import io.a2a.spec.AgentCapabilities;
import io.a2a.spec.AgentCard;
import io.a2a.spec.AgentInterface;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.a2a.common.model.RocketMQRequest;
import org.apache.rocketmq.a2a.common.model.RocketMQResponse;
import org.apache.rocketmq.a2a.common.util.RocketMQUtil;
import org.apache.rocketmq.a2a.transport.config.RocketMQTransportConfig;
import org.apache.rocketmq.client.apis.consumer.LitePushConsumer;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import static org.apache.rocketmq.a2a.common.constant.RocketMQA2AConstant.ROCKETMQ_PROTOCOL;

/** In-process broker boundary that exercises the real transport and response dispatcher. */
final class TransportTestSupport implements AutoCloseable {
    final String namespace = "transport-test-" + UUID.randomUUID();
    final String taskId = namespace + "-task";
    final CountDownLatch sent = new CountDownLatch(1);
    final ExecutorService receiver = Executors.newSingleThreadExecutor();
    SendAction onSend = request -> { };
    Future<?> delivery;

    @FunctionalInterface
    interface SendAction {
        void accept(RocketMQRequest request) throws Exception;
    }

    RocketMQTransport transport(boolean extendedCard) {
        Producer producer = (Producer) Proxy.newProxyInstance(Producer.class.getClassLoader(),
            new Class<?>[] {Producer.class}, (proxy, method, args) -> {
                if ("send".equals(method.getName())) {
                    ByteBuffer body = ((Message) args[0]).getBody().duplicate();
                    byte[] bytes = new byte[body.remaining()];
                    body.get(bytes);
                    RocketMQRequest request = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8), RocketMQRequest.class);
                    try {
                        onSend.accept(request);
                        return receipt();
                    } finally {
                        sent.countDown();
                    }
                }
                if ("toString".equals(method.getName())) {
                    return "test-producer";
                }
                throw new UnsupportedOperationException(method.getName());
            });
        LitePushConsumer consumer = (LitePushConsumer) Proxy.newProxyInstance(LitePushConsumer.class.getClassLoader(),
            new Class<?>[] {LitePushConsumer.class}, (proxy, method, args) -> null);
        RocketMQUtil.ROCKETMQ_PRODUCER_MAP.put(namespace, new ConcurrentHashMap<>(Map.of("agent-task", producer)));
        RocketMQUtil.ROCKETMQ_CONSUMER_MAP.put(namespace, new ConcurrentHashMap<>(Map.of("agent-replies", consumer)));
        String url = "http://localhost:8081/" + namespace + "/agent-task";
        AgentCard card = new AgentCard.Builder().name("test agent").description("transport regression test")
            .url(url).version("1.0.0").protocolVersion("0.3.0")
            .capabilities(new AgentCapabilities.Builder().build())
            .defaultInputModes(List.of("text")).defaultOutputModes(List.of("text"))
            .skills(Collections.emptyList()).preferredTransport(ROCKETMQ_PROTOCOL)
            .additionalInterfaces(List.of(new AgentInterface(ROCKETMQ_PROTOCOL, url)))
            .supportsAuthenticatedExtendedCard(extendedCard).build();
        RocketMQTransportConfig config = new RocketMQTransportConfig("", "", "localhost:8081", namespace,
            "agent-replies", "reply-group", "agent-task", null, "test-session", false);
        return new RocketMQTransport(config, card);
    }

    void replyNow(RocketMQRequest request, String responseBody) throws Exception {
        RocketMQResponse response = RocketMQResponse.builder().requestId(request.getRequestId())
            .responseBody(responseBody).stream(false).build();
        Method dispatcher = RocketMQUtil.class.getDeclaredMethod("processNonStreamResult", RocketMQResponse.class, String.class);
        dispatcher.setAccessible(true);
        dispatcher.invoke(null, response, namespace);
    }

    void replyAfterRegistration(RocketMQRequest request, String responseBody) {
        delivery = receiver.submit(() -> {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!RocketMQUtil.MESSAGE_RESPONSE_MAP.getOrDefault(namespace, Collections.emptyMap())
                .containsKey(request.getRequestId())) {
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException("Response was never registered");
                }
                Thread.sleep(1);
            }
            replyNow(request, responseBody);
            return null;
        });
    }

    String taskResponse() {
        return "{\"jsonrpc\":\"2.0\",\"id\":\"test\",\"result\":{\"kind\":\"task\",\"id\":\"" + taskId
            + "\",\"contextId\":\"context\",\"status\":{\"state\":\"working\"}}}";
    }

    boolean hasPendingResponses() {
        return !RocketMQUtil.MESSAGE_RESPONSE_MAP.getOrDefault(namespace, Collections.emptyMap()).isEmpty();
    }

    private static SendReceipt receipt() {
        MessageId messageId = (MessageId) Proxy.newProxyInstance(MessageId.class.getClassLoader(),
            new Class<?>[] {MessageId.class}, (proxy, method, args) -> "test-message-id");
        return (SendReceipt) Proxy.newProxyInstance(SendReceipt.class.getClassLoader(),
            new Class<?>[] {SendReceipt.class}, (proxy, method, args) -> "getMessageId".equals(method.getName()) ? messageId : null);
    }

    @Override
    public void close() throws Exception {
        receiver.shutdownNow();
        receiver.awaitTermination(5, TimeUnit.SECONDS);
        RocketMQUtil.ROCKETMQ_PRODUCER_MAP.remove(namespace);
        RocketMQUtil.ROCKETMQ_CONSUMER_MAP.remove(namespace);
        RocketMQUtil.MESSAGE_RESPONSE_MAP.remove(namespace);
        RocketMQUtil.LITE_TOPIC_USE_DEFAULT_RECOVER_MAP.remove(namespace);
        RocketMQUtil.TASK_SERVER_RECEIPT_MAP.remove(taskId);
    }
}
