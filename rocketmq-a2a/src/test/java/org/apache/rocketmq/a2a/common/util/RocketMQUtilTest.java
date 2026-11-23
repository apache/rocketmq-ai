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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.a2a.common.future.A2AResponseFuture;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RocketMQUtilTest {
    @Test(timeout = 10000)
    public void removesPendingResponseAfterSuccess() throws Exception {
        verifyCleanup("success");
    }

    @Test(timeout = 10000)
    public void removesPendingResponseAfterInterruption() throws Exception {
        verifyCleanup("interrupt");
    }

    @Test(timeout = 10000)
    public void removesPendingResponseAfterExceptionalCompletion() throws Exception {
        verifyCleanup("exception");
    }

    private void verifyCleanup(String mode) throws Exception {
        String namespace = "test-" + UUID.randomUUID();
        String requestId = "request";
        CountDownLatch done = new CountDownLatch(1);
        CompletableFuture<Object> outcome = new CompletableFuture<>();
        Thread waiter = new Thread(() -> {
            try {
                outcome.complete(RocketMQUtil.getResult(requestId, namespace, null));
            } catch (Exception e) {
                outcome.complete(e);
            } finally {
                done.countDown();
            }
        });
        try {
            waiter.start();
            A2AResponseFuture pending = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (pending == null && System.nanoTime() < deadline) {
                Map<String, A2AResponseFuture> responses = RocketMQUtil.MESSAGE_RESPONSE_MAP.get(namespace);
                if (responses != null) {
                    pending = responses.get(requestId);
                }
                if (pending == null) {
                    Thread.sleep(1);
                }
            }
            assertTrue("Request must be registered before completion", pending != null);
            if ("interrupt".equals(mode)) {
                waiter.interrupt();
            } else if ("exception".equals(mode)) {
                pending.getCompletableFuture().completeExceptionally(new IllegalStateException("test failure"));
            } else {
                pending.getCompletableFuture().complete("response");
            }
            assertTrue("Waiter must terminate", done.await(5, TimeUnit.SECONDS));
            Object result = outcome.get();
            if ("interrupt".equals(mode)) {
                assertTrue(result instanceof InterruptedException);
            } else if ("exception".equals(mode)) {
                assertTrue(result instanceof ExecutionException);
                assertTrue(((ExecutionException) result).getCause() instanceof IllegalStateException);
            } else {
                assertEquals("response", result);
            }
            assertFalse("Completed request must not remain in the pending map",
                RocketMQUtil.MESSAGE_RESPONSE_MAP.get(namespace).containsKey(requestId));
        } finally {
            waiter.interrupt();
            waiter.join(1000);
            RocketMQUtil.MESSAGE_RESPONSE_MAP.remove(namespace);
        }
    }
}
