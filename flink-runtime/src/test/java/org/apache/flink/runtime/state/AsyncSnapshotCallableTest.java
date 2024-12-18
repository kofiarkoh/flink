/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.util.Preconditions;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AsyncSnapshotCallable}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class AsyncSnapshotCallableTest {

    private static final String METHOD_CALL = "callInternal";
    private static final String METHOD_LOG = "logAsyncSnapshotComplete";
    private static final String METHOD_CLEANUP = "cleanupProvidedResources";
    private static final String METHOD_CANCEL = "cancel";
    private static final String SUCCESS = "Success!";

    private CloseableRegistry ownerRegistry;
    private TestBlockingCloseable testProvidedResource;
    private TestBlockingCloseable testBlocker;
    private TestAsyncSnapshotCallable testAsyncSnapshotCallable;
    private FutureTask<String> task;

    @BeforeEach
    void setup() throws IOException {
        ownerRegistry = new CloseableRegistry();
        testProvidedResource = new TestBlockingCloseable();
        testBlocker = new TestBlockingCloseable();
        testAsyncSnapshotCallable =
                new TestAsyncSnapshotCallable(testProvidedResource, testBlocker);
        task = testAsyncSnapshotCallable.toAsyncSnapshotFutureTask(ownerRegistry);
        assertThat(ownerRegistry.getNumberOfRegisteredCloseables()).isOne();
    }

    @AfterEach
    void finalChecks() {
        assertThat(testProvidedResource.isClosed()).isTrue();
        assertThat(ownerRegistry.getNumberOfRegisteredCloseables()).isZero();
    }

    @Test
    void testNormalRun() throws Exception {

        Thread runner = startTask(task);

        while (testBlocker.getWaitersCount() < 1) {
            Thread.sleep(1L);
        }

        testBlocker.unblockSuccessfully();

        runner.join();

        assertThat(task.get()).isEqualTo(SUCCESS);
        assertThat(testAsyncSnapshotCallable.getInvocationOrder())
                .containsExactly(METHOD_CALL, METHOD_LOG, METHOD_CLEANUP);

        assertThat(testBlocker.isClosed()).isTrue();
    }

    @Test
    void testExceptionRun() throws Exception {

        testBlocker.introduceException();
        Thread runner = startTask(task);

        while (testBlocker.getWaitersCount() < 1) {
            Thread.sleep(1L);
        }

        testBlocker.unblockSuccessfully();
        assertThatThrownBy(task::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IOException.class);

        runner.join();

        assertThat(testAsyncSnapshotCallable.getInvocationOrder())
                .containsExactly(METHOD_CALL, METHOD_CLEANUP);

        assertThat(testBlocker.isClosed()).isTrue();
    }

    @Test
    void testCancelRun() throws Exception {

        Thread runner = startTask(task);

        while (testBlocker.getWaitersCount() < 1) {
            Thread.sleep(1L);
        }

        task.cancel(true);
        testBlocker.unblockExceptionally();

        assertThatThrownBy(task::get).isInstanceOf(CancellationException.class);

        runner.join();

        assertThat(testAsyncSnapshotCallable.getInvocationOrder())
                .containsExactly(METHOD_CALL, METHOD_CANCEL, METHOD_CLEANUP);
        assertThat(testProvidedResource.isClosed()).isTrue();
        assertThat(testBlocker.isClosed()).isTrue();
    }

    @Test
    void testCloseRun() throws Exception {

        Thread runner = startTask(task);

        while (testBlocker.getWaitersCount() < 1) {
            Thread.sleep(1L);
        }

        ownerRegistry.close();

        assertThatThrownBy(task::get).isInstanceOf(CancellationException.class);

        runner.join();

        assertThat(testAsyncSnapshotCallable.getInvocationOrder())
                .containsExactly(METHOD_CALL, METHOD_CANCEL, METHOD_CLEANUP);
        assertThat(testBlocker.isClosed()).isTrue();
    }

    @Test
    void testCancelBeforeRun() throws Exception {

        task.cancel(true);

        Thread runner = startTask(task);

        assertThatThrownBy(task::get).isInstanceOf(CancellationException.class);

        runner.join();

        assertThat(testAsyncSnapshotCallable.getInvocationOrder())
                .containsExactly(METHOD_CANCEL, METHOD_CLEANUP);
        assertThat(testProvidedResource.isClosed()).isTrue();
    }

    private Thread startTask(Runnable task) {
        Thread runner = new Thread(task);
        runner.start();
        return runner;
    }

    /** Test implementation of {@link AsyncSnapshotCallable}. */
    private static class TestAsyncSnapshotCallable extends AsyncSnapshotCallable<String> {

        @Nonnull private final TestBlockingCloseable providedResource;
        @Nonnull private final TestBlockingCloseable blockingResource;
        @Nonnull private final List<String> invocationOrder;

        TestAsyncSnapshotCallable(
                @Nonnull TestBlockingCloseable providedResource,
                @Nonnull TestBlockingCloseable blockingResource) {

            this.providedResource = providedResource;
            this.blockingResource = blockingResource;
            this.invocationOrder = new ArrayList<>();
        }

        @Override
        protected String callInternal() throws Exception {

            addInvocation(METHOD_CALL);
            snapshotCloseableRegistry.registerCloseable(blockingResource);
            try {
                blockingResource.simulateBlockingOperation();
            } finally {
                if (snapshotCloseableRegistry.unregisterCloseable(blockingResource)) {
                    blockingResource.close();
                }
            }

            return SUCCESS;
        }

        @Override
        protected void cleanupProvidedResources() {
            addInvocation(METHOD_CLEANUP);
            providedResource.close();
        }

        @Override
        protected void logAsyncSnapshotComplete(long startTime) {
            invocationOrder.add(METHOD_LOG);
        }

        @Override
        protected void cancel() {
            addInvocation(METHOD_CANCEL);
            super.cancel();
        }

        @Nonnull
        public List<String> getInvocationOrder() {
            synchronized (invocationOrder) {
                return new ArrayList<>(invocationOrder);
            }
        }

        private void addInvocation(@Nonnull String invocation) {
            synchronized (invocationOrder) {
                invocationOrder.add(invocation);
            }
        }
    }

    /** Mix of a {@link Closeable} and some {@link OneShotLatch} functionality for testing. */
    private static class TestBlockingCloseable implements Closeable {

        private final OneShotLatch blockerLatch = new OneShotLatch();
        private boolean closed = false;
        private boolean unblocked = false;
        private boolean exceptionally = false;

        public void simulateBlockingOperation() throws IOException {
            while (!unblocked) {
                try {
                    blockerLatch.await();
                } catch (InterruptedException e) {
                    blockerLatch.reset();
                }
            }
            if (exceptionally) {
                throw new IOException("Closed in block");
            }
        }

        @Override
        public void close() {
            Preconditions.checkState(!closed);
            this.closed = true;
            unblockExceptionally();
        }

        public boolean isClosed() {
            return closed;
        }

        public void unblockExceptionally() {
            introduceException();
            unblock();
        }

        public void unblockSuccessfully() {
            unblock();
        }

        private void unblock() {
            this.unblocked = true;
            blockerLatch.trigger();
        }

        public void introduceException() {
            this.exceptionally = true;
        }

        public int getWaitersCount() {
            return blockerLatch.getWaitersCount();
        }
    }
}
