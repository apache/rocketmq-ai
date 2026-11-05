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

import jakarta.enterprise.event.Event;
import jakarta.enterprise.util.TypeLiteral;
import java.lang.annotation.Annotation;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.a2aproject.sdk.extras.queuemanager.replicated.core.ReplicatedEventQueueItem;
import org.a2aproject.sdk.extras.queuemanager.replicated.core.ReplicatedQueueManager;
import org.a2aproject.sdk.server.events.EventQueue;
import org.a2aproject.sdk.server.events.EventQueueItem;
import org.a2aproject.sdk.server.events.MainEventBus;
import org.a2aproject.sdk.server.events.MainEventBusProcessor;
import org.a2aproject.sdk.server.events.QueueClosedEvent;
import org.a2aproject.sdk.server.tasks.InMemoryTaskStore;
import org.a2aproject.sdk.server.tasks.PushNotificationSender;
import org.a2aproject.sdk.spec.StreamingEventKind;
import org.a2aproject.sdk.spec.Task;
import org.a2aproject.sdk.spec.TaskState;
import org.a2aproject.sdk.spec.TaskStatus;
import org.a2aproject.sdk.spec.TaskStatusUpdateEvent;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-JVM demo simulating two A2A server replicas (A and B) backed by
 * {@link LiteTopicReplicationStrategy}. Each replica has its own consumer group, task store, event
 * bus and replicated queue manager — the only shared state is the LiteTopic channels on the broker.
 *
 * <p>Scenario: replica B handles task-1 first and a client opens its stream there (subscribing its
 * channel), then replica A enqueues a status event for task-1. With dedicated groups the broker
 * delivers A's event to B, whose mirror queue must receive it. The poison pill then unsubscribes B.
 */
public class TwoReplicaDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwoReplicaDemo.class);

    private static final String TASK_ID = "task-1";

    public static void main(String[] args) throws Exception {
        String endpoints = args.length > 0 ? args[0] : "localhost:8081";
        String parentTopic = args.length > 1 ? args[1] : "a2a-parent-topic";
        String accessKey = args.length > 2 ? args[2] : null;
        String secretKey = args.length > 3 ? args[3] : null;

        Replica replicaA = new Replica("A", "a2a-replication-group-0", endpoints, parentTopic, accessKey, secretKey);
        Replica replicaB = new Replica("B", "a2a-replication-group-1", endpoints, parentTopic, accessKey, secretKey);
        try {
            // Both replicas share the persisted task state in a real deployment; simulate by saving
            // task-1 into each replica's task store.
            Task task1 = Task.builder()
                .id(TASK_ID)
                .contextId("ctx-1")
                .status(new TaskStatus(TaskState.TASK_STATE_WORKING))
                .build();
            replicaA.taskStore.save(task1, false);
            replicaB.taskStore.save(task1, false);

            // Step 1: replica B handles task-1 first and enqueues a status event. Enqueuing does not
            // subscribe (send() never subscribes); a client opening the task stream on B does.
            EventQueue queueB = replicaB.queueManager.createOrTap(TASK_ID);
            queueB.enqueueEvent(statusEvent(TASK_ID, TaskState.TASK_STATE_WORKING));
            replicaB.hook.onSubscribe(TASK_ID, null);
            await("B subscribes channel t-" + TASK_ID, () -> replicaB.strategy.isSubscribed(TASK_ID));
            LOGGER.info("[STEP 1] replica B subscribed channel for {} (client stream opened)", TASK_ID);

            // Step 2: replica A handles the same task and enqueues a NON-final status event
            // (final states skip the enqueue hook in the SDK; they are replicated via
            // ReplicatedQueueManager.onTaskFinalized instead). A does not subscribe (no client on
            // A); the broker delivers once per group: A drops its own copy, B's mirror queue
            // receives it.
            EventQueue queueA = replicaA.queueManager.createOrTap(TASK_ID);
            queueA.enqueueEvent(statusEvent(TASK_ID, TaskState.TASK_STATE_INPUT_REQUIRED));
            await("B mirror queue receives A's event", () -> queueB.size() >= 2);
            queueB.dequeueEventItem(0); // drain B's own working event
            EventQueueItem received = queueB.dequeueEventItem(0);
            TaskStatusUpdateEvent mirrored = (TaskStatusUpdateEvent) received.getEvent();
            LOGGER.info("[STEP 2] replica B mirrored A's event: task={} state={}",
                mirrored.taskId(), mirrored.status().state());

            // Step 3: poison pill. The SDK sends QueueClosedEvent via onTaskFinalized (a direct
            // strategy.send, not an enqueue); mirror that path here.
            replicaA.strategy.send(TASK_ID, new QueueClosedEvent(TASK_ID));
            await("B receives poison pill and unsubscribes", () -> !replicaB.strategy.isSubscribed(TASK_ID));
            LOGGER.info("[STEP 3] replica B unsubscribed after poison pill");

            // Assertions
            boolean pass = queueB.size() >= 1
                && mirrored.status().state() == TaskState.TASK_STATE_INPUT_REQUIRED
                && !replicaB.strategy.isSubscribed(TASK_ID);
            LOGGER.info("=== DEMO RESULT: {} ===", pass ? "PASS" : "FAIL");
            if (!pass) {
                throw new IllegalStateException("Replication verification failed");
            }
        } finally {
            replicaA.close();
            replicaB.close();
        }
    }

    private static TaskStatusUpdateEvent statusEvent(String taskId, TaskState state) {
        return TaskStatusUpdateEvent.builder()
            .taskId(taskId)
            .contextId("ctx-1")
            .status(new TaskStatus(state))
            .build();
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("Timed out waiting for: " + what);
    }

    /**
     * One simulated A2A server replica: private task store, event bus, strategy, lifecycle hook and
     * queue manager, exactly the beans a single CDI application instance would hold.
     */
    private static class Replica implements AutoCloseable {
        final String name;
        final InMemoryTaskStore taskStore = new InMemoryTaskStore();
        final MainEventBus bus = new MainEventBus();
        final LiteTopicReplicationStrategy strategy;
        final LiteTopicTaskStreamLifecycleHook hook;
        final ReplicatedQueueManager queueManager;
        final MainEventBusProcessor processor;

        Replica(String name, String group, String endpoints, String parentTopic, String accessKey,
            String secretKey) throws Exception {
            this.name = name;
            LiteTopicReplicationConfig config =
                new LiteTopicReplicationConfig(endpoints, accessKey, secretKey, group, parentTopic, "t-");
            // Bridge: in a CDI app the strategy fires CDI events observed by ReplicatedQueueManager;
            // here we wire the same seam manually.
            BridgeEvent bridge = new BridgeEvent(this);
            this.strategy = new LiteTopicReplicationStrategy(config, ClientServiceProvider.loadService(), bridge);
            this.hook = new LiteTopicTaskStreamLifecycleHook(strategy);
            this.queueManager = new ReplicatedQueueManager(strategy, taskStore, bus);
            this.processor = new MainEventBusProcessor(bus, taskStore, new NoopPushSender(), queueManager);
            this.strategy.start();
            this.processor.start();
            LOGGER.info("[{}] replica started, group={}, instance={}", name, group, strategy.getInstanceId());
        }

        @Override
        public void close() {
            try {
                strategy.stop();
            } catch (Exception e) {
                LOGGER.warn("[{}] close error", name, e);
            }
        }
    }

    /** CDI event seam: fire(...) forwards to ReplicatedQueueManager.onReplicatedEvent. */
    private static class BridgeEvent implements Event<ReplicatedEventQueueItem> {
        private final Replica replica;

        BridgeEvent(Replica replica) {
            this.replica = replica;
        }

        @Override
        public void fire(ReplicatedEventQueueItem event) {
            replica.queueManager.onReplicatedEvent(event);
        }

        @Override
        public <U extends ReplicatedEventQueueItem> CompletionStage<U> fireAsync(U event) {
            fire(event);
            return CompletableFuture.completedFuture(event);
        }

        @Override
        public <U extends ReplicatedEventQueueItem> CompletionStage<U> fireAsync(U event,
            jakarta.enterprise.event.NotificationOptions options) {
            return fireAsync(event);
        }

        @Override
        public Event<ReplicatedEventQueueItem> select(Annotation... qualifiers) {
            return this;
        }

        @Override
        public <U extends ReplicatedEventQueueItem> Event<U> select(Class<U> subtype, Annotation... qualifiers) {
            return (Event<U>) this;
        }

        @Override
        public <U extends ReplicatedEventQueueItem> Event<U> select(TypeLiteral<U> subtype, Annotation... qualifiers) {
            return (Event<U>) this;
        }
    }

    /** Push notifications are irrelevant for the replication path; no-op them. */
    private static class NoopPushSender implements PushNotificationSender {
        @Override
        public void sendNotification(StreamingEventKind event, Task task) {
        }
    }
}
