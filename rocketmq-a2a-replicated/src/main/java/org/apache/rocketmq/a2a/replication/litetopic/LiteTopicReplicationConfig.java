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

import java.util.Objects;

/**
 * Configuration for {@link LiteTopicReplicationStrategy}.
 *
 * <p>A LiteTopic deployment exposes one or more endpoints (RocketMQ proxy, gRPC). Every replicated
 * event is written to a per-task channel {@code <channelPrefix><taskId>} under the parent topic, so the
 * parent topic must be a topic bound to the LiteTopic resource of the target deployment.
 */
public class LiteTopicReplicationConfig {

    /** Environment variable for RocketMQ endpoints, see {@code ClientConfiguration#setEndpoints}. */
    public static final String ENV_ENDPOINTS = "ROCKETMQ_ENDPOINTS";
    /** Environment variable for the access key; leave unset for anonymous access. */
    public static final String ENV_ACCESS_KEY = "ROCKETMQ_ACCESS_KEY";
    /** Environment variable for the secret key; leave unset for anonymous access. */
    public static final String ENV_SECRET_KEY = "ROCKETMQ_SECRET_KEY";
    /**
     * Environment variable for the consumer group of the inbound LitePushConsumer. Each instance must
     * be assigned its own pre-created group: Lite broker dispatch is once per group, so dedicated
     * groups broadcast each replicated event to every instance. Sharing one group across instances
     * turns delivery into random single-instance dispatch and mirrors fall behind.
     */
    public static final String ENV_CONSUMER_GROUP = "A2A_REPLICATION_CONSUMER_GROUP";
    /** Environment variable for the parent topic of the LiteTopic channels. */
    public static final String ENV_PARENT_TOPIC = "A2A_REPLICATION_PARENT_TOPIC";
    /** Environment variable for the channel name prefix, default {@value #DEFAULT_CHANNEL_PREFIX}. */
    public static final String ENV_CHANNEL_PREFIX = "A2A_REPLICATION_CHANNEL_PREFIX";

    public static final String DEFAULT_CHANNEL_PREFIX = "t-";
    public static final String DEFAULT_CONSUMER_GROUP = "a2a-replication-group";

    private final String endpoints;
    private final String accessKey;
    private final String secretKey;
    private final String consumerGroup;
    private final String parentTopic;
    private final String channelPrefix;

    public LiteTopicReplicationConfig(String endpoints, String consumerGroup, String parentTopic, String channelPrefix) {
        this(endpoints, null, null, consumerGroup, parentTopic, channelPrefix);
    }

    public LiteTopicReplicationConfig(String endpoints, String accessKey, String secretKey, String consumerGroup,
        String parentTopic, String channelPrefix) {
        this.endpoints = Objects.requireNonNull(endpoints, "endpoints");
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.consumerGroup = Objects.requireNonNull(consumerGroup, "consumerGroup");
        this.parentTopic = Objects.requireNonNull(parentTopic, "parentTopic");
        this.channelPrefix = Objects.requireNonNull(channelPrefix, "channelPrefix");
    }

    /** Builds a config from environment variables, falling back to defaults where sensible. */
    public static LiteTopicReplicationConfig fromEnv() {
        String endpoints = System.getenv(ENV_ENDPOINTS);
        if (endpoints == null) {
            throw new IllegalStateException("Missing required env var " + ENV_ENDPOINTS);
        }
        String parentTopic = System.getenv(ENV_PARENT_TOPIC);
        if (parentTopic == null) {
            throw new IllegalStateException("Missing required env var " + ENV_PARENT_TOPIC);
        }
        String consumerGroup = System.getenv().getOrDefault(ENV_CONSUMER_GROUP, DEFAULT_CONSUMER_GROUP);
        String channelPrefix = System.getenv().getOrDefault(ENV_CHANNEL_PREFIX, DEFAULT_CHANNEL_PREFIX);
        return new LiteTopicReplicationConfig(endpoints, System.getenv(ENV_ACCESS_KEY),
            System.getenv(ENV_SECRET_KEY), consumerGroup, parentTopic, channelPrefix);
    }

    public String getEndpoints() {
        return endpoints;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public String getParentTopic() {
        return parentTopic;
    }

    public String getChannelPrefix() {
        return channelPrefix;
    }

    /** Maps a taskId to its per-task channel name. */
    public String channelName(String taskId) {
        return channelPrefix + taskId;
    }
}
