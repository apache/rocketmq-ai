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
import io.a2a.spec.DeleteTaskPushNotificationConfigParams;
import io.a2a.util.Utils;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RocketMQTransportDeleteNotificationTest {
    @Test(timeout = 10000)
    public void propagatesServerRpcError() throws Exception {
        try (TransportTestSupport support = new TransportTestSupport()) {
            support.onSend = request -> support.replyAfterRegistration(request,
                "{\"jsonrpc\":\"2.0\",\"id\":\"test\",\"error\":{\"code\":-32602,\"message\":\"configuration not found\"}}");
            try {
                support.transport(false).deleteTaskPushNotificationConfigurations(params(support.taskId), null);
                fail("Deleting a missing configuration must not appear successful");
            } catch (A2AClientException expected) {
                assertTrue(expected.getCause() instanceof A2AClientException);
                assertTrue(expected.getCause().getMessage().contains("configuration not found"));
            }
            support.delivery.get(3, TimeUnit.SECONDS);
            assertFalse(support.hasPendingResponses());
        }
    }

    @Test(timeout = 10000)
    public void acceptsSuccessfulNullResult() throws Exception {
        try (TransportTestSupport support = new TransportTestSupport()) {
            support.onSend = request -> support.replyAfterRegistration(request,
                "{\"jsonrpc\":\"2.0\",\"id\":\"test\",\"result\":null}");
            support.transport(false).deleteTaskPushNotificationConfigurations(params(support.taskId), null);
            support.delivery.get(3, TimeUnit.SECONDS);
            assertFalse(support.hasPendingResponses());
        }
    }

    @Test(timeout = 10000)
    public void rejectsMalformedResponse() throws Exception {
        try (TransportTestSupport support = new TransportTestSupport()) {
            support.onSend = request -> support.replyAfterRegistration(request, "not-json");
            try {
                support.transport(false).deleteTaskPushNotificationConfigurations(params(support.taskId), null);
                fail("An unreadable response must not appear successful");
            } catch (A2AClientException expected) {
                assertTrue(expected.getCause() instanceof com.fasterxml.jackson.core.JsonProcessingException);
            }
            support.delivery.get(3, TimeUnit.SECONDS);
        }
    }

    private static DeleteTaskPushNotificationConfigParams params(String taskId) throws Exception {
        return Utils.OBJECT_MAPPER.readValue("{\"id\":\"" + taskId
            + "\",\"pushNotificationConfigId\":\"config-1\"}", DeleteTaskPushNotificationConfigParams.class);
    }
}
