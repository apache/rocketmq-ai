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
package org.apache.rocketmq.a2a.replication.litetopic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.enterprise.event.Event;
import jakarta.enterprise.util.TypeLiteral;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.a2aproject.sdk.extras.queuemanager.replicated.core.ReplicatedEventQueueItem;
import org.a2aproject.sdk.jsonrpc.common.json.JsonUtil;
import org.a2aproject.sdk.server.events.QueueClosedEvent;
import org.a2aproject.sdk.spec.TaskState;
import org.a2aproject.sdk.spec.TaskStatus;
import org.a2aproject.sdk.spec.TaskStatusUpdateEvent;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.junit.jupiter.api.Test;

class LiteTopicReplicationStrategyTest {

    private static final LiteTopicReplicationConfig CONFIG =
        new LiteTopicReplicationConfig("localhost:8080", "test-group", "a2a-parent-topic", "t-");

    @Test
    void channelNameMapsTaskIdToPerTaskChannel() {
        assertEquals("t-task-1", CONFIG.channelName("task-1"));
    }

    @Test
    void statusUpdateEventSurvivesJsonRoundtrip() throws org.a2aproject.sdk.jsonrpc.common.json.JsonProcessingException {
        TaskStatusUpdateEvent event = TaskStatusUpdateEvent.builder()
            .taskId("task-1")
            .contextId("ctx-1")
            .status(new TaskStatus(TaskState.TASK_STATE_COMPLETED))
            .build();
        ReplicatedEventQueueItem item = new ReplicatedEventQueueItem("task-1", event);

        String json = JsonUtil.toJson(item);
        ReplicatedEventQueueItem restored = JsonUtil.fromJson(json, ReplicatedEventQueueItem.class);

        assertEquals("task-1", restored.getTaskId());
        assertTrue(restored.hasEvent());
        assertFalse(restored.isClosedEvent());
        assertInstanceOf(TaskStatusUpdateEvent.class, restored.getStreamingEvent());
        assertEquals(TaskState.TASK_STATE_COMPLETED,
            ((TaskStatusUpdateEvent) restored.getStreamingEvent()).status().state());
    }

    @Test
    void poisonPillSurvivesJsonRoundtrip() throws org.a2aproject.sdk.jsonrpc.common.json.JsonProcessingException {
        ReplicatedEventQueueItem item = new ReplicatedEventQueueItem("task-2", new QueueClosedEvent("task-2"));

        String json = JsonUtil.toJson(item);
        ReplicatedEventQueueItem restored = JsonUtil.fromJson(json, ReplicatedEventQueueItem.class);

        assertEquals("task-2", restored.getTaskId());
        assertTrue(restored.isClosedEvent());
        assertInstanceOf(QueueClosedEvent.class, restored.getEvent());
    }

    @Test
    void selfReceivedEventIsDroppedWithoutFiring() throws org.a2aproject.sdk.jsonrpc.common.json.JsonProcessingException {
        CollectingCdiEvent cdiEvent = new CollectingCdiEvent();
        LiteTopicReplicationStrategy strategy = newStrategy(cdiEvent);
        String json = JsonUtil.toJson(new ReplicatedEventQueueItem("task-3", new QueueClosedEvent("task-3")));

        ConsumeResult result = strategy.onMessage(new StubMessageView(json,
            Map.of(LiteTopicReplicationStrategy.PROP_INSTANCE_ID, strategy.getInstanceId())));

        assertEquals(ConsumeResult.SUCCESS, result);
        assertTrue(cdiEvent.fired.isEmpty());
    }

    @Test
    void foreignEventIsFired() throws org.a2aproject.sdk.jsonrpc.common.json.JsonProcessingException {
        CollectingCdiEvent cdiEvent = new CollectingCdiEvent();
        LiteTopicReplicationStrategy strategy = newStrategy(cdiEvent);
        TaskStatusUpdateEvent event = TaskStatusUpdateEvent.builder()
            .taskId("task-4")
            .contextId("ctx-4")
            .status(new TaskStatus(TaskState.TASK_STATE_WORKING))
            .build();
        String json = JsonUtil.toJson(new ReplicatedEventQueueItem("task-4", event));

        ConsumeResult result = strategy.onMessage(new StubMessageView(json,
            Map.of(LiteTopicReplicationStrategy.PROP_INSTANCE_ID, "another-instance")));

        assertEquals(ConsumeResult.SUCCESS, result);
        assertEquals(1, cdiEvent.fired.size());
        assertEquals("task-4", cdiEvent.fired.get(0).getTaskId());
    }

    @Test
    void malformedMessageIsAcknowledgedWithoutThrowing() {
        CollectingCdiEvent cdiEvent = new CollectingCdiEvent();
        LiteTopicReplicationStrategy strategy = newStrategy(cdiEvent);

        ConsumeResult result = strategy.onMessage(new StubMessageView("not-json", Map.of()));

        assertEquals(ConsumeResult.SUCCESS, result);
        assertTrue(cdiEvent.fired.isEmpty());
    }

    private static LiteTopicReplicationStrategy newStrategy(CollectingCdiEvent cdiEvent) {
        return new LiteTopicReplicationStrategy(CONFIG, null, cdiEvent);
    }

    /** Minimal CDI event stub that records fired payloads. */
    private static class CollectingCdiEvent implements Event<ReplicatedEventQueueItem> {
        final List<ReplicatedEventQueueItem> fired = new ArrayList<>();

        @Override
        public void fire(ReplicatedEventQueueItem event) {
            fired.add(event);
        }

        @Override
        public <U extends ReplicatedEventQueueItem> CompletionStage<U> fireAsync(U event) {
            return fireAsync(event, null);
        }

        @Override
        public <U extends ReplicatedEventQueueItem> CompletionStage<U> fireAsync(U event,
            jakarta.enterprise.event.NotificationOptions options) {
            fired.add(event);
            return java.util.concurrent.CompletableFuture.completedFuture(event);
        }

        @Override
        public Event<ReplicatedEventQueueItem> select(java.lang.annotation.Annotation... qualifiers) {
            return this;
        }

        @Override
        public <U extends ReplicatedEventQueueItem> Event<U> select(Class<U> subtype,
            java.lang.annotation.Annotation... qualifiers) {
            return (Event<U>) this;
        }

        @Override
        public <U extends ReplicatedEventQueueItem> Event<U> select(TypeLiteral<U> subtype,
            java.lang.annotation.Annotation... qualifiers) {
            return (Event<U>) this;
        }
    }

    /** Minimal MessageView stub exposing only body and properties. */
    private static class StubMessageView implements MessageView {
        private final ByteBuffer body;
        private final Map<String, String> properties;

        StubMessageView(String body, Map<String, String> properties) {
            this.body = ByteBuffer.wrap(body.getBytes(StandardCharsets.UTF_8));
            this.properties = new HashMap<>(properties);
        }

        @Override
        public MessageId getMessageId() {
            return null;
        }

        @Override
        public String getTopic() {
            return CONFIG.getParentTopic();
        }

        @Override
        public ByteBuffer getBody() {
            return body;
        }

        @Override
        public Map<String, String> getProperties() {
            return properties;
        }

        @Override
        public java.util.Optional<String> getTag() {
            return java.util.Optional.empty();
        }

        @Override
        public java.util.Collection<String> getKeys() {
            return java.util.Collections.emptyList();
        }

        @Override
        public java.util.Optional<String> getMessageGroup() {
            return java.util.Optional.empty();
        }

        @Override
        public java.util.Optional<String> getLiteTopic() {
            return java.util.Optional.empty();
        }

        @Override
        public java.util.Optional<Long> getDeliveryTimestamp() {
            return java.util.Optional.empty();
        }

        @Override
        public java.util.Optional<Integer> getPriority() {
            return java.util.Optional.empty();
        }

        @Override
        public String getBornHost() {
            return null;
        }

        @Override
        public long getBornTimestamp() {
            return 0L;
        }

        @Override
        public int getDeliveryAttempt() {
            return 1;
        }
    }
}