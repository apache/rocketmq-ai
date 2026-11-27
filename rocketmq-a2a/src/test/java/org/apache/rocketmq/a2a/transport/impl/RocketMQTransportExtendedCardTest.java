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

import io.a2a.spec.A2AClientException;
import io.a2a.spec.AgentCard;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RocketMQTransportExtendedCardTest {
    @Test(timeout = 10000)
    public void fetchesExtendedCardWhenSuppliedPublicCardAdvertisesSupport() throws Exception {
        try (TransportTestSupport support = new TransportTestSupport()) {
            support.onSend = request -> support.replyAfterRegistration(request,
                "{\"jsonrpc\":\"2.0\",\"id\":\"test\",\"result\":{"
                    + "\"name\":\"authenticated agent\",\"description\":\"extended card\","
                    + "\"url\":\"http://localhost:8081/a2a\",\"version\":\"1.0.0\",\"protocolVersion\":\"0.3.0\","
                    + "\"capabilities\":{},\"defaultInputModes\":[\"text\"],\"defaultOutputModes\":[\"text\"],\"skills\":[]}}");
            AgentCard card = support.transport(true).getAgentCard(null);
            assertEquals("authenticated agent", card.name());
            support.delivery.get(3, TimeUnit.SECONDS);
            assertEquals(0, support.sent.getCount());
        }
    }

    @Test
    public void returnsPublicCardWithoutRpcWhenExtendedCardsAreUnsupported() throws Exception {
        try (TransportTestSupport support = new TransportTestSupport()) {
            support.onSend = request -> fail("The public card does not require a network request");
            assertEquals("test agent", support.transport(false).getAgentCard(null).name());
            assertEquals(1, support.sent.getCount());
        }
    }

    @Test(timeout = 10000)
    public void propagatesExtendedCardAuthorizationError() throws Exception {
        try (TransportTestSupport support = new TransportTestSupport()) {
            support.onSend = request -> support.replyAfterRegistration(request,
                "{\"jsonrpc\":\"2.0\",\"id\":\"test\",\"error\":{\"code\":-32001,\"message\":\"authorization required\"}}");
            try {
                support.transport(true).getAgentCard(null);
                fail("An authorization error must not silently return the public card");
            } catch (A2AClientException expected) {
                assertTrue(expected.getCause().getMessage().contains("authorization required"));
            }
            support.delivery.get(3, TimeUnit.SECONDS);
        }
    }
}
