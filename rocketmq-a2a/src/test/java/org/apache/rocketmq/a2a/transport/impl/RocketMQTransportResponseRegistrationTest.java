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
import io.a2a.spec.Task;
import io.a2a.spec.TaskQueryParams;
import io.a2a.util.Utils;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RocketMQTransportResponseRegistrationTest {
    @Test(timeout = 10000)
    public void receivesReplyBeforeProducerSendReturns() throws Exception {
        try (TransportTestSupport support = new TransportTestSupport()) {
            support.onSend = request -> support.replyNow(request, support.taskResponse());
            RocketMQTransport transport = support.transport(false);
            ExecutorService caller = Executors.newSingleThreadExecutor();
            Future<Task> call = caller.submit(() -> transport.getTask(params(support.taskId), null));
            try {
                assertEquals(support.taskId, call.get(3, TimeUnit.SECONDS).getId());
                assertFalse(support.hasPendingResponses());
            } finally {
                call.cancel(true);
                caller.shutdownNow();
                assertTrue(caller.awaitTermination(3, TimeUnit.SECONDS));
            }
        }
    }

    @Test(timeout = 10000)
    public void receivesDelayedReplyAndRemovesPendingRequest() throws Exception {
        try (TransportTestSupport support = new TransportTestSupport()) {
            support.onSend = request -> support.replyAfterRegistration(request, support.taskResponse());
            assertEquals(support.taskId, support.transport(false).getTask(params(support.taskId), null).getId());
            support.delivery.get(3, TimeUnit.SECONDS);
            assertFalse(support.hasPendingResponses());
        }
    }

    @Test
    public void sendFailureRemovesRegisteredRequest() throws Exception {
        try (TransportTestSupport support = new TransportTestSupport()) {
            support.onSend = request -> { throw new IllegalStateException("send failed"); };
            try {
                support.transport(false).getTask(params(support.taskId), null);
                fail("Send failure must reach caller");
            } catch (A2AClientException expected) {
                assertTrue(expected.getCause() instanceof IllegalStateException);
            }
            assertFalse(support.hasPendingResponses());
        }
    }

    @Test(timeout = 10000)
    public void interruptedWaitRemovesRegisteredRequest() throws Exception {
        try (TransportTestSupport support = new TransportTestSupport()) {
            RocketMQTransport transport = support.transport(false);
            CountDownLatch finished = new CountDownLatch(1);
            Thread caller = new Thread(() -> {
                try {
                    transport.getTask(params(support.taskId), null);
                } catch (Exception expected) {
                    // An interrupted blocking call is expected to fail.
                } finally {
                    finished.countDown();
                }
            });
            try {
                caller.start();
                assertTrue(support.sent.await(3, TimeUnit.SECONDS));
                caller.interrupt();
                assertTrue(finished.await(3, TimeUnit.SECONDS));
                assertFalse(support.hasPendingResponses());
            } finally {
                caller.interrupt();
                caller.join(3000);
            }
        }
    }

    private static TaskQueryParams params(String taskId) throws Exception {
        return Utils.OBJECT_MAPPER.readValue("{\"id\":\"" + taskId + "\"}", TaskQueryParams.class);
    }
}
