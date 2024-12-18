/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.runtime.execution.CancelTaskException;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.isInBlockingBufferRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the destruction of a {@link LocalBufferPool}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class LocalBufferPoolDestroyTest {
    @Test
    void testRequestAfterDestroy() {
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(1, 4096);
        LocalBufferPool localBufferPool = new LocalBufferPool(networkBufferPool, 1);
        localBufferPool.lazyDestroy();

        assertThatThrownBy(localBufferPool::requestBuffer)
                .withFailMessage("Call should have failed with an CancelTaskException")
                .isInstanceOf(CancelTaskException.class);
    }

    /**
     * Tests that a blocking request fails properly if the buffer pool is destroyed.
     *
     * <p>Starts a Thread, which triggers an unsatisfiable blocking buffer request. After making
     * sure that the Thread is actually waiting in the blocking call, the buffer pool is destroyed
     * and we check whether the request Thread threw the expected Exception.
     */
    @Test
    void testDestroyWhileBlockingRequest() throws Exception {
        AtomicReference<Exception> asyncException = new AtomicReference<>();

        NetworkBufferPool networkBufferPool = null;
        LocalBufferPool localBufferPool = null;

        try {
            networkBufferPool = new NetworkBufferPool(1, 4096);
            localBufferPool = new LocalBufferPool(networkBufferPool, 1);

            // Drain buffer pool
            assertThat(localBufferPool.requestBuffer()).isNotNull();
            assertThat(localBufferPool.requestBuffer()).isNull();

            // Start request Thread
            Thread thread = new Thread(new BufferRequestTask(localBufferPool, asyncException));
            thread.start();

            // Wait for request
            boolean success = false;

            for (int i = 0; i < 50; i++) {
                StackTraceElement[] stackTrace = thread.getStackTrace();
                success = isInBlockingBufferRequest(stackTrace);

                if (success) {
                    break;
                } else {
                    // Retry
                    Thread.sleep(500);
                }
            }

            // Verify that Thread was in blocking request
            assertThat(success)
                    .withFailMessage("Did not trigger blocking buffer request.")
                    .isTrue();

            // Destroy the buffer pool
            localBufferPool.lazyDestroy();

            // Wait for Thread to finish
            thread.join();

            // Verify expected Exception
            assertThat(asyncException.get())
                    .withFailMessage("Did not throw expected Exception")
                    .isInstanceOf(CancelTaskException.class);
        } finally {
            if (localBufferPool != null) {
                localBufferPool.lazyDestroy();
            }

            if (networkBufferPool != null) {
                networkBufferPool.destroyAllBufferPools();
                networkBufferPool.destroy();
            }
        }
    }

    /** Task triggering a blocking buffer request (the test assumes that no buffer is available). */
    private static class BufferRequestTask implements Runnable {

        private final BufferPool bufferPool;
        private final AtomicReference<Exception> asyncException;

        BufferRequestTask(BufferPool bufferPool, AtomicReference<Exception> asyncException) {
            this.bufferPool = bufferPool;
            this.asyncException = asyncException;
        }

        @Override
        public void run() {
            try {
                String msg = "Test assumption violated: expected no available buffer";
                assertThat(bufferPool.requestBuffer()).withFailMessage(msg).isNull();

                bufferPool.requestBufferBuilderBlocking();
            } catch (Exception t) {
                asyncException.set(t);
            }
        }
    }
}
