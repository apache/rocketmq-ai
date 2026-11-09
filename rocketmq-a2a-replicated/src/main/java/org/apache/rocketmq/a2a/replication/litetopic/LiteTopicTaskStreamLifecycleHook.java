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

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import org.a2aproject.sdk.server.events.StreamCloseHandle;
import org.a2aproject.sdk.server.events.TaskStreamLifecycleHook;
import org.a2aproject.sdk.spec.Event;

/**
 * Bridges the SDK's stream lifecycle into {@link LiteTopicReplicationStrategy} subscription.
 *
 * <p>Replaces the SDK's no-op {@code DefaultTaskStreamLifecycleHook} (same
 * {@code @Alternative @Priority} pattern the SDK uses to shadow {@code InMemoryQueueManager}). A
 * client opening a task stream subscribes its channel — the only subscription trigger, since
 * {@code send()} replicates without subscribing; without an offset option the broker replays the
 * channel history, so the stream sees the full history even if this instance never replicated an
 * event for the task.
 *
 * <p>Stream closes do not unsubscribe: the subscription is kept until the poison pill
 * ({@code QueueClosedEvent}) to avoid replay churn when several clients watch the same task. Channel
 * TTL on the broker reclaims storage after the pill.
 */
@ApplicationScoped
@Alternative
@Priority(1)
public class LiteTopicTaskStreamLifecycleHook implements TaskStreamLifecycleHook {

    private final LiteTopicReplicationStrategy strategy;

    @Inject
    public LiteTopicTaskStreamLifecycleHook(LiteTopicReplicationStrategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public void onSubscribe(String taskId, StreamCloseHandle handle) {
        strategy.ensureSubscribed(taskId);
    }

    @Override
    public void onUnsubscribe(String taskId, StreamCloseHandle handle) {
        // Deliberately keep the channel subscription: the poison pill owns the teardown.
    }

    @Override
    public void onEvent(String taskId, Event event, StreamCloseHandle handle) {
        // No channel action needed on per-event notifications.
    }
}
