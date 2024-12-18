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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link DefaultExecutionGraphCache}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class DefaultExecutionGraphCacheTest {

    private static ExecutionGraphInfo expectedExecutionGraphInfo;
    private static final JobID expectedJobId = new JobID();

    @BeforeAll
    static void setup() {
        expectedExecutionGraphInfo =
                new ExecutionGraphInfo(new ArchivedExecutionGraphBuilder().build());
    }

    /** Tests that we can cache AccessExecutionGraphs over multiple accesses. */
    @Test
    void testExecutionGraphCaching() throws Exception {
        final Duration timeout = Duration.ofMillis(100L);
        final Duration timeToLive = Duration.ofHours(1L);

        final CountingRestfulGateway restfulGateway =
                createCountingRestfulGateway(
                        expectedJobId,
                        CompletableFuture.completedFuture(expectedExecutionGraphInfo));

        try (ExecutionGraphCache executionGraphCache =
                new DefaultExecutionGraphCache(timeout, timeToLive)) {
            CompletableFuture<ExecutionGraphInfo> executionGraphInfoFuture =
                    executionGraphCache.getExecutionGraphInfo(expectedJobId, restfulGateway);

            assertThatFuture(executionGraphInfoFuture)
                    .eventuallySucceeds()
                    .isEqualTo(expectedExecutionGraphInfo);

            executionGraphInfoFuture =
                    executionGraphCache.getExecutionGraphInfo(expectedJobId, restfulGateway);

            assertThatFuture(executionGraphInfoFuture)
                    .eventuallySucceeds()
                    .isEqualTo(expectedExecutionGraphInfo);

            assertThat(restfulGateway.getNumRequestJobCalls()).isOne();
        }
    }

    /** Tests that an AccessExecutionGraph is invalidated after its TTL expired. */
    @Test
    void testExecutionGraphEntryInvalidation() throws Exception {
        final Duration timeout = Duration.ofMillis(100L);
        final Duration timeToLive = Duration.ofMillis(1L);

        final CountingRestfulGateway restfulGateway =
                createCountingRestfulGateway(
                        expectedJobId,
                        CompletableFuture.completedFuture(expectedExecutionGraphInfo),
                        CompletableFuture.completedFuture(expectedExecutionGraphInfo));

        try (ExecutionGraphCache executionGraphCache =
                new DefaultExecutionGraphCache(timeout, timeToLive)) {
            CompletableFuture<ExecutionGraphInfo> executionGraphInfoFuture =
                    executionGraphCache.getExecutionGraphInfo(expectedJobId, restfulGateway);

            assertThatFuture(executionGraphInfoFuture)
                    .eventuallySucceeds()
                    .isEqualTo(expectedExecutionGraphInfo);

            // sleep for the TTL
            Thread.sleep(timeToLive.toMillis() * 5L);

            CompletableFuture<ExecutionGraphInfo> executionGraphInfoFuture2 =
                    executionGraphCache.getExecutionGraphInfo(expectedJobId, restfulGateway);

            assertThatFuture(executionGraphInfoFuture2)
                    .eventuallySucceeds()
                    .isEqualTo(expectedExecutionGraphInfo);

            assertThat(restfulGateway.getNumRequestJobCalls()).isEqualTo(2);
        }
    }

    /**
     * Tests that a failure in requesting an AccessExecutionGraph from the gateway, will not create
     * a cache entry --> another cache request will trigger a new gateway request.
     */
    @Test
    void testImmediateCacheInvalidationAfterFailure() throws Exception {
        final Duration timeout = Duration.ofMillis(100L);
        final Duration timeToLive = Duration.ofHours(1L);

        // let's first answer with a JobNotFoundException and then only with the correct result
        final CountingRestfulGateway restfulGateway =
                createCountingRestfulGateway(
                        expectedJobId,
                        FutureUtils.completedExceptionally(
                                new FlinkJobNotFoundException(expectedJobId)),
                        CompletableFuture.completedFuture(expectedExecutionGraphInfo));

        try (ExecutionGraphCache executionGraphCache =
                new DefaultExecutionGraphCache(timeout, timeToLive)) {
            CompletableFuture<ExecutionGraphInfo> executionGraphFuture =
                    executionGraphCache.getExecutionGraphInfo(expectedJobId, restfulGateway);

            assertThatThrownBy(() -> executionGraphFuture.get())
                    .hasCauseInstanceOf(FlinkException.class);

            CompletableFuture<ExecutionGraphInfo> executionGraphFuture2 =
                    executionGraphCache.getExecutionGraphInfo(expectedJobId, restfulGateway);

            assertThatFuture(executionGraphFuture2)
                    .eventuallySucceeds()
                    .isEqualTo(expectedExecutionGraphInfo);
        }
    }

    /**
     * Tests that cache entries are cleaned up when their TTL has expired upon calling {@link
     * DefaultExecutionGraphCache#cleanup()}.
     */
    @Test
    void testCacheEntryCleanup() throws Exception {
        final Duration timeout = Duration.ofMillis(100L);
        final Duration timeToLive = Duration.ofMillis(1L);
        final JobID expectedJobId2 = new JobID();
        final ExecutionGraphInfo expectedExecutionGraphInfo2 =
                new ExecutionGraphInfo(new ArchivedExecutionGraphBuilder().build());

        final AtomicInteger requestJobCalls = new AtomicInteger(0);
        final TestingRestfulGateway restfulGateway =
                new TestingRestfulGateway.Builder()
                        .setRequestExecutionGraphInfoFunction(
                                jobId -> {
                                    requestJobCalls.incrementAndGet();
                                    if (jobId.equals(expectedJobId)) {
                                        return CompletableFuture.completedFuture(
                                                expectedExecutionGraphInfo);
                                    } else if (jobId.equals(expectedJobId2)) {
                                        return CompletableFuture.completedFuture(
                                                expectedExecutionGraphInfo2);
                                    } else {
                                        throw new AssertionError("Invalid job id received.");
                                    }
                                })
                        .build();

        try (ExecutionGraphCache executionGraphCache =
                new DefaultExecutionGraphCache(timeout, timeToLive)) {

            CompletableFuture<ExecutionGraphInfo> executionGraph1Future =
                    executionGraphCache.getExecutionGraphInfo(expectedJobId, restfulGateway);

            CompletableFuture<ExecutionGraphInfo> executionGraph2Future =
                    executionGraphCache.getExecutionGraphInfo(expectedJobId2, restfulGateway);

            assertThatFuture(executionGraph1Future)
                    .eventuallySucceeds()
                    .isEqualTo(expectedExecutionGraphInfo);

            assertThatFuture(executionGraph2Future)
                    .eventuallySucceeds()
                    .isEqualTo(expectedExecutionGraphInfo2);

            assertThat(requestJobCalls.get()).isEqualTo(2);

            Thread.sleep(timeToLive.toMillis());

            executionGraphCache.cleanup();

            assertThat(executionGraphCache.size()).isZero();
        }
    }

    /** Tests that concurrent accesses only trigger a single AccessExecutionGraph request. */
    @Test
    void testConcurrentAccess() throws Exception {
        final Duration timeout = Duration.ofMillis(100L);
        final Duration timeToLive = Duration.ofHours(1L);

        final CountingRestfulGateway restfulGateway =
                createCountingRestfulGateway(
                        expectedJobId,
                        CompletableFuture.completedFuture(expectedExecutionGraphInfo));

        final int numConcurrentAccesses = 10;

        final ArrayList<CompletableFuture<ExecutionGraphInfo>> executionGraphFutures =
                new ArrayList<>(numConcurrentAccesses);

        final ExecutorService executor =
                java.util.concurrent.Executors.newFixedThreadPool(numConcurrentAccesses);

        try (ExecutionGraphCache executionGraphCache =
                new DefaultExecutionGraphCache(timeout, timeToLive)) {
            for (int i = 0; i < numConcurrentAccesses; i++) {
                CompletableFuture<ExecutionGraphInfo> executionGraphFuture =
                        CompletableFuture.supplyAsync(
                                        () ->
                                                executionGraphCache.getExecutionGraphInfo(
                                                        expectedJobId, restfulGateway),
                                        executor)
                                .thenCompose(Function.identity());

                executionGraphFutures.add(executionGraphFuture);
            }

            final CompletableFuture<Collection<ExecutionGraphInfo>> allExecutionGraphFutures =
                    FutureUtils.combineAll(executionGraphFutures);

            Collection<ExecutionGraphInfo> allExecutionGraphs = allExecutionGraphFutures.get();

            for (ExecutionGraphInfo executionGraph : allExecutionGraphs) {
                assertThat(executionGraph).isEqualTo(expectedExecutionGraphInfo);
            }

            assertThat(restfulGateway.getNumRequestJobCalls()).isOne();
        } finally {
            ExecutorUtils.gracefulShutdown(5000L, TimeUnit.MILLISECONDS, executor);
        }
    }

    private CountingRestfulGateway createCountingRestfulGateway(
            JobID jobId, CompletableFuture<ExecutionGraphInfo>... accessExecutionGraphs) {
        final ConcurrentLinkedQueue<CompletableFuture<ExecutionGraphInfo>> queue =
                new ConcurrentLinkedQueue<>(Arrays.asList(accessExecutionGraphs));
        return new CountingRestfulGateway(jobId, ignored -> queue.poll());
    }

    /**
     * {@link RestfulGateway} implementation which counts the number of {@link #requestJob(JobID,
     * Duration)} calls.
     */
    private static @ExtendWith(CTestJUnit5Extension.class) @CTestClass class CountingRestfulGateway
            extends TestingRestfulGateway {

        private final JobID expectedJobId;

        private AtomicInteger numRequestJobCalls = new AtomicInteger(0);

        private CountingRestfulGateway(
                JobID expectedJobId,
                Function<JobID, CompletableFuture<ExecutionGraphInfo>> requestJobFunction) {
            this.expectedJobId = Preconditions.checkNotNull(expectedJobId);
            this.requestExecutionGraphInfoFunction = Preconditions.checkNotNull(requestJobFunction);
        }

        @Override
        public CompletableFuture<ExecutionGraphInfo> requestExecutionGraphInfo(
                JobID jobId, Duration timeout) {
            assertThat(jobId).isEqualTo(expectedJobId);
            numRequestJobCalls.incrementAndGet();
            return super.requestExecutionGraphInfo(jobId, timeout);
        }

        public int getNumRequestJobCalls() {
            return numRequestJobCalls.get();
        }
    }
}
