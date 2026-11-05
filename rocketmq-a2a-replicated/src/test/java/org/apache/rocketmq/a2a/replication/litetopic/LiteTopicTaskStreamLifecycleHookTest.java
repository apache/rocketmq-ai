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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class LiteTopicTaskStreamLifecycleHookTest {

    private static final LiteTopicReplicationConfig CONFIG =
        new LiteTopicReplicationConfig("localhost:8080", "test-group", "a2a-parent-topic", "t-");

    @Test
    void onSubscribeSubscribesTaskChannel() {
        RecordingStrategy strategy = new RecordingStrategy();
        LiteTopicTaskStreamLifecycleHook hook = new LiteTopicTaskStreamLifecycleHook(strategy);

        hook.onSubscribe("task-9", null);

        assertEquals(List.of("t-task-9"), strategy.subscribedChannels);
    }

    @Test
    void repeatedStreamOpensDelegateEachTime() {
        RecordingStrategy strategy = new RecordingStrategy();
        LiteTopicTaskStreamLifecycleHook hook = new LiteTopicTaskStreamLifecycleHook(strategy);

        hook.onSubscribe("task-9", null);
        hook.onSubscribe("task-9", null);

        // The hook forwards every open; idempotency is the strategy's responsibility.
        assertEquals(List.of("t-task-9", "t-task-9"), strategy.subscribedChannels);
    }

    @Test
    void onUnsubscribeDoesNotTouchSubscription() {
        RecordingStrategy strategy = new RecordingStrategy();
        LiteTopicTaskStreamLifecycleHook hook = new LiteTopicTaskStreamLifecycleHook(strategy);
        hook.onSubscribe("task-9", null);

        hook.onUnsubscribe("task-9", null);

        // The channel subscription survives stream closes; the poison pill owns the teardown.
        assertEquals(List.of("t-task-9"), strategy.subscribedChannels);
    }

    @Test
    void onEventIsNoOp() {
        RecordingStrategy strategy = new RecordingStrategy();
        LiteTopicTaskStreamLifecycleHook hook = new LiteTopicTaskStreamLifecycleHook(strategy);

        hook.onEvent("task-9", null, null);

        assertTrue(strategy.subscribedChannels.isEmpty());
    }

    /** Strategy stub recording the subscription calls the hook is expected to make. */
    private static class RecordingStrategy extends LiteTopicReplicationStrategy {
        final List<String> subscribedChannels = new ArrayList<>();

        RecordingStrategy() {
            super(CONFIG, null, null);
        }

        @Override
        public void ensureSubscribed(String taskId) {
            subscribedChannels.add(CONFIG.channelName(taskId));
        }
    }
}
