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
import java.util.List;
import org.apache.rocketmq.a2a.common.model.RocketMQResource;
import org.junit.Test;

import static org.apache.rocketmq.a2a.common.constant.RocketMQA2AConstant.ROCKETMQ_PROTOCOL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class RocketMQAgentCardTest {
    private static final String ROCKETMQ_URL = "http://127.0.0.1:8081/demo/AgentTask";

    @Test
    public void parsesPreferredRocketMQWhenAdditionalInterfacesAreOmitted() {
        assertRocketMQResource(card(ROCKETMQ_PROTOCOL, ROCKETMQ_URL).build());
    }

    @Test
    public void parsesPreferredRocketMQWhenAdditionalInterfacesAreEmpty() {
        assertRocketMQResource(card(ROCKETMQ_PROTOCOL, ROCKETMQ_URL).additionalInterfaces(List.of()).build());
    }

    @Test
    public void fallsBackToAnAdditionalRocketMQInterface() {
        AgentCard agentCard = card("JSONRPC", "http://127.0.0.1:9999")
            .additionalInterfaces(List.of(new AgentInterface(ROCKETMQ_PROTOCOL, ROCKETMQ_URL)))
            .build();
        assertRocketMQResource(agentCard);
    }

    @Test
    public void returnsNullWhenNoRocketMQInterfaceExists() {
        assertNull(RocketMQUtil.parseAgentCardAddition(card("JSONRPC", "http://127.0.0.1:9999").build()));
    }

    private static AgentCard.Builder card(String transport, String url) {
        return new AgentCard.Builder()
            .name("Test agent")
            .description("RocketMQ transport test")
            .version("1.0")
            .preferredTransport(transport)
            .url(url)
            .capabilities(new AgentCapabilities.Builder().build())
            .defaultInputModes(List.of("text/plain"))
            .defaultOutputModes(List.of("text/plain"))
            .skills(List.of());
    }

    private static void assertRocketMQResource(AgentCard agentCard) {
        RocketMQResource resource = RocketMQUtil.parseAgentCardAddition(agentCard);
        assertNotNull(resource);
        assertEquals("demo", resource.getNamespace());
        assertEquals("AgentTask", resource.getTopic());
    }
}
