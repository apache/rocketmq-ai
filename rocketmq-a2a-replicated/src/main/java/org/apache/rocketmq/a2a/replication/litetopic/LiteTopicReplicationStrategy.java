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

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.a2aproject.sdk.extras.queuemanager.replicated.core.ReplicatedEventQueueItem;
import org.a2aproject.sdk.extras.queuemanager.replicated.core.ReplicationStrategy;
import org.a2aproject.sdk.jsonrpc.common.json.JsonUtil;
import org.a2aproject.sdk.spec.Event;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.LitePushConsumer;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A2A {@link ReplicationStrategy} backed by RocketMQ LiteTopic.
 *
 * <p>Outbound: {@link #send(String, Event)} serializes the event as a {@link ReplicatedEventQueueItem}
 * and writes it to the per-task channel {@code <channelPrefix><taskId>} under the parent topic — the
 * taskId is the natural routing key, so delivery is precise instead of the shared-topic broadcast of
 * {@code ReactiveMessagingReplicationStrategy}.
 *
 * <p>Inbound: a singleton {@link LitePushConsumer} receives replicated events and fires them as CDI
 * {@code Event<ReplicatedEventQueueItem>}, observed by {@code ReplicatedQueueManager#onReplicatedEvent}
 * (same seam as the official reactive-messaging strategy).
 *
 * <p>Subscription lifecycle: a client opening the task stream subscribes the channel (bridged by
 * {@link LiteTopicTaskStreamLifecycleHook}); the poison pill ({@code QueueClosedEvent}) unsubscribes
 * it. {@link #send(String, Event)} never subscribes: replication and mirroring are orthogonal.
 * Subscribing without an offset option replays the channel history, so a late subscriber
 * deterministically back-fills events instead of relying on a grace period.
 *
 * <p>Deployment model: every instance needs its own pre-created consumer group
 * ({@code A2A_REPLICATION_CONSUMER_GROUP}). Lite broker dispatch is once per group, so dedicated
 * groups give the broadcast the mirror model relies on; a shared group turns delivery into random
 * single-instance dispatch and mirrors silently fall behind.
 *
 * <p>Self-receipt: with a dedicated group, events produced by this instance always return to it; the
 * per-instance property drops them on receipt, since the local queue already holds them.
 */
@ApplicationScoped
public class LiteTopicReplicationStrategy implements ReplicationStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(LiteTopicReplicationStrategy.class);

    /** Message property marking the producing instance, used to drop self-received events. */
    public static final String PROP_INSTANCE_ID = "a2a-replication-instance";

    private final LiteTopicReplicationConfig config;
    private final ClientServiceProvider provider;
    private final jakarta.enterprise.event.Event<ReplicatedEventQueueItem> cdiEvent;
    private final String instanceId;

    private Producer producer;
    private LitePushConsumer consumer;
    private volatile boolean started = false;

    @Inject
    public LiteTopicReplicationStrategy(LiteTopicReplicationConfig config,
        ClientServiceProvider provider,
        jakarta.enterprise.event.Event<ReplicatedEventQueueItem> cdiEvent) {
        this.config = config;
        this.provider = provider;
        this.cdiEvent = cdiEvent;
        this.instanceId = UUID.randomUUID().toString();
    }

    @PostConstruct
    public void start() throws ClientException {
        if (started) {
            return;
        }
        ClientConfigurationBuilder clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(config.getEndpoints());
        if (config.getAccessKey() != null) {
            clientConfiguration.setCredentialProvider(
                new StaticSessionCredentialsProvider(config.getAccessKey(), config.getSecretKey()));
        }
        ClientConfiguration clientConfig = clientConfiguration.build();
        this.producer = provider.newProducerBuilder()
            .setClientConfiguration(clientConfig)
            .build();
        this.consumer = provider.newLitePushConsumerBuilder()
            .setClientConfiguration(clientConfig)
            .setConsumerGroup(config.getConsumerGroup())
            .bindTopic(config.getParentTopic())
            .setMessageListener(this::onMessage)
            .build();
        this.started = true;
        LOGGER.info("LiteTopicReplicationStrategy started, instance={}, parentTopic={}, consumerGroup={}",
            instanceId, config.getParentTopic(), config.getConsumerGroup());
    }

    @PreDestroy
    public void stop() throws IOException {
        if (!started) {
            return;
        }
        consumer.close();
        producer.close();
        started = false;
        LOGGER.info("LiteTopicReplicationStrategy stopped, instance={}", instanceId);
    }

    @Override
    public void send(String taskId, Event event) {
        if (!started) {
            throw new IllegalStateException("LiteTopicReplicationStrategy is not started");
        }
        ReplicatedEventQueueItem item = new ReplicatedEventQueueItem(taskId, event);
        try {
            String json = JsonUtil.toJson(item);
            Message message = provider.newMessageBuilder()
                .setTopic(config.getParentTopic())
                .setLiteTopic(config.channelName(taskId))
                .setBody(json.getBytes(StandardCharsets.UTF_8))
                .addProperty(PROP_INSTANCE_ID, instanceId)
                .build();
            producer.send(message);
        } catch (ClientException | org.a2aproject.sdk.jsonrpc.common.json.JsonProcessingException e) {
            throw new IllegalStateException("Failed to replicate event for task " + taskId, e);
        }
    }

    /**
     * Subscribes the per-task channel, invoked when a client opens the task stream
     * ({@link LiteTopicTaskStreamLifecycleHook#onSubscribe(String, org.a2aproject.sdk.server.events.StreamCloseHandle)}).
     * Idempotent via the client's internal guard; without an offset option the broker replays the
     * channel history, back-filling events produced before subscription.
     */
    public void ensureSubscribed(String taskId) {
        String channel = config.channelName(taskId);
        try {
            consumer.subscribeLite(channel);
        } catch (ClientException e) {
            LOGGER.warn("Failed to subscribe channel {}, retried on next demand signal for task {}", channel, taskId, e);
        }
    }

    private void unsubscribe(String taskId) {
        String channel = config.channelName(taskId);
        try {
            consumer.unsubscribeLite(channel);
            LOGGER.debug("Unsubscribed channel {}", channel);
        } catch (ClientException e) {
            LOGGER.warn("Failed to unsubscribe channel {}", channel, e);
        }
    }

    /** Visible for testing. */
    ConsumeResult onMessage(MessageView messageView) {
        String json = StandardCharsets.UTF_8.decode(messageView.getBody()).toString();
        try {
            ReplicatedEventQueueItem item = JsonUtil.fromJson(json, ReplicatedEventQueueItem.class);
            if (instanceId.equals(messageView.getProperties().get(PROP_INSTANCE_ID))) {
                // Self-received: the local queue already holds this event, drop to avoid duplicate enqueue.
                return ConsumeResult.SUCCESS;
            }
            String taskId = item.getTaskId();
            cdiEvent.fire(item);
            if (item.isClosedEvent()) {
                // Poison pill: the task reached a final state on the owning instance, stop following it.
                unsubscribe(taskId);
            }
        } catch (Exception e) {
            // Never throw: one malformed message must not stop the whole consumer.
            LOGGER.error("Failed to process replicated event: {}", json, e);
        }
        return ConsumeResult.SUCCESS;
    }

    /** Visible for testing. */
    String getInstanceId() {
        return instanceId;
    }

    /** Visible for testing. */
    boolean isSubscribed(String taskId) {
        return consumer.getLiteTopicSet().contains(config.channelName(taskId));
    }
}
