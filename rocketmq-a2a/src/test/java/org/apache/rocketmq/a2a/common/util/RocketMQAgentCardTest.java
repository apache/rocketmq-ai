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

import io.a2a.spec.AgentCapabilities;
import io.a2a.spec.AgentCard;
import io.a2a.spec.AgentInterface;
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.a2a.common.model.RocketMQResource;
import org.junit.Test;
import static org.apache.rocketmq.a2a.common.constant.RocketMQA2AConstant.ROCKETMQ_PROTOCOL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class RocketMQAgentCardTest {
    @Test
    public void acceptsPreferredTransportWithoutAdditionalInterfaces() {
        AgentCard card = card("http://localhost:8081/ns/primary", ROCKETMQ_PROTOCOL).build();
        assertResource(RocketMQUtil.parseAgentCardAddition(card), "ns", "primary");
    }

    @Test
    public void acceptsPreferredTransportWithEmptyAdditionalInterfaces() {
        AgentCard card = card("http://localhost:8081/ns/primary", ROCKETMQ_PROTOCOL)
            .additionalInterfaces(Collections.emptyList()).build();
        assertResource(RocketMQUtil.parseAgentCardAddition(card), "ns", "primary");
    }

    @Test
    public void prefersPrimaryRocketMQTransportOverAdditionalInterface() {
        AgentCard card = card("http://localhost:8081/ns/primary", ROCKETMQ_PROTOCOL)
            .additionalInterfaces(List.of(new AgentInterface(ROCKETMQ_PROTOCOL, "http://localhost:8081/ns/fallback")))
            .build();
        assertResource(RocketMQUtil.parseAgentCardAddition(card), "ns", "primary");
    }

    @Test
    public void fallsBackToAdditionalRocketMQInterface() {
        AgentCard card = card("http://localhost:8081/a2a", "JSONRPC")
            .additionalInterfaces(List.of(new AgentInterface(ROCKETMQ_PROTOCOL, "http://localhost:8081/ns/fallback")))
            .build();
        assertResource(RocketMQUtil.parseAgentCardAddition(card), "ns", "fallback");
    }

    @Test
    public void acceptsEmptyNamespaceForSelfHostedBroker() {
        AgentCard card = card("http://localhost:8081//primary", ROCKETMQ_PROTOCOL).build();
        assertResource(RocketMQUtil.parseAgentCardAddition(card), "", "primary");
    }

    @Test
    public void rejectsCardWithoutRocketMQInterface() {
        assertNull(RocketMQUtil.parseAgentCardAddition(card("http://localhost:8081/a2a", "JSONRPC").build()));
        assertNull(RocketMQUtil.parseAgentCardAddition(card("http://localhost:8081/invalid", ROCKETMQ_PROTOCOL).build()));
        assertNull(RocketMQUtil.parseAgentCardAddition(null));
    }

    private static AgentCard.Builder card(String url, String preferredTransport) {
        return new AgentCard.Builder()
            .name("test agent").description("AgentCard discovery regression test")
            .url(url).version("1.0.0").protocolVersion("0.3.0")
            .capabilities(new AgentCapabilities.Builder().build())
            .defaultInputModes(List.of("text")).defaultOutputModes(List.of("text"))
            .skills(Collections.emptyList()).preferredTransport(preferredTransport);
    }

    private static void assertResource(RocketMQResource resource, String namespace, String topic) {
        assertNotNull("A valid preferred RocketMQ interface must be discoverable", resource);
        assertEquals("localhost:8081", resource.getEndpoint());
        assertEquals(namespace, resource.getNamespace());
        assertEquals(topic, resource.getTopic());
    }
}
