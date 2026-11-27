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
package org.apache.rocketmq.a2a.server;

import com.fasterxml.jackson.databind.JsonNode;
import io.a2a.spec.A2AClientException;
import io.a2a.spec.InvalidParamsError;
import io.a2a.spec.JSONRPCErrorResponse;
import io.a2a.util.Utils;
import java.lang.reflect.Method;
import org.apache.rocketmq.a2a.common.model.RocketMQResponse;
import org.apache.rocketmq.a2a.common.util.RocketMQUtil;
import org.junit.Test;
import static org.apache.rocketmq.a2a.common.constant.RocketMQA2AConstant.SEND_MESSAGE_RESPONSE_REFERENCE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RocketMQErrorResponseTest {
    @Test
    public void serializesErrorAsJsonObjectAndPreservesIds() throws Exception {
        RocketMQResponse response = errorResponse("rpc-id", "transport-id");
        JsonNode body = Utils.OBJECT_MAPPER.readTree(response.getResponseBody());
        assertTrue("A JSON-RPC response must be an object, not an encoded JSON string", body.isObject());
        assertEquals("2.0", body.path("jsonrpc").asText());
        assertEquals("rpc-id", body.path("id").asText());
        assertEquals(-32602, body.path("error").path("code").asInt());
        assertEquals("transport-id", response.getRequestId());
        assertTrue(response.isEnd());
        assertFalse(response.isStream());
    }

    @Test
    public void clientReceivesOriginalProtocolError() throws Exception {
        try {
            RocketMQUtil.unmarshalResponse(errorResponse("rpc-id", "transport-id").getResponseBody(),
                SEND_MESSAGE_RESPONSE_REFERENCE);
            fail("A protocol error must reach the caller");
        } catch (A2AClientException expected) {
            assertTrue(expected.getCause() instanceof InvalidParamsError);
        }
    }

    @Test
    public void preservesNumericJsonRpcId() throws Exception {
        JsonNode body = Utils.OBJECT_MAPPER.readTree(errorResponse(37, "transport-id").getResponseBody());
        assertTrue(body.isObject());
        assertEquals(37L, body.path("id").asLong());
    }

    private static RocketMQResponse errorResponse(Object id, String requestId) throws Exception {
        Method method = RocketMQA2AServerRoutes.class.getDeclaredMethod("buildErrorResponse",
            JSONRPCErrorResponse.class, String.class);
        method.setAccessible(true);
        return (RocketMQResponse) method.invoke(new RocketMQA2AServerRoutes(),
            new JSONRPCErrorResponse(id, new InvalidParamsError()), requestId);
    }
}
