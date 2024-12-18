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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.runtime.blocklist.NoOpBlocklistHandler;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.io.network.partition.NoOpResourceManagerPartitionTracker;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.TestingSlotManagerBuilder;
import org.apache.flink.runtime.resourcemanager.utils.MockResourceManagerRuntimeServices;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcServiceExtension;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the Standalone Resource Manager. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class StandaloneResourceManagerTest {

    @RegisterExtension
    public static final AllCallbackWrapper<TestingRpcServiceExtension>
            RPC_SERVICE_EXTENSION_WRAPPER =
                    new AllCallbackWrapper<>(new TestingRpcServiceExtension());

    private static final Duration TIMEOUT = Duration.ofSeconds(10L);

    private final TestingFatalErrorHandler fatalErrorHandler = new TestingFatalErrorHandler();

    @Test
    public void testStartupPeriod() throws Exception {
        final LinkedBlockingQueue<Boolean> setFailUnfulfillableRequestInvokes =
                new LinkedBlockingQueue<>();
        final SlotManager slotManager =
                new TestingSlotManagerBuilder()
                        .setSetFailUnfulfillableRequestConsumer(
                                setFailUnfulfillableRequestInvokes::add)
                        .createSlotManager();
        final TestingStandaloneResourceManager rm =
                createResourceManager(Duration.ofMillis(1L), slotManager);

        assertThat(setFailUnfulfillableRequestInvokes.take()).isFalse();
        assertThat(setFailUnfulfillableRequestInvokes.take()).isTrue();

        rm.close();
    }

    @Test
    public void testNoStartupPeriod() throws Exception {
        final LinkedBlockingQueue<Boolean> setFailUnfulfillableRequestInvokes =
                new LinkedBlockingQueue<>();
        final SlotManager slotManager =
                new TestingSlotManagerBuilder()
                        .setSetFailUnfulfillableRequestConsumer(
                                setFailUnfulfillableRequestInvokes::add)
                        .createSlotManager();
        final TestingStandaloneResourceManager rm =
                createResourceManager(Duration.ofMillis(-1L), slotManager);

        assertThat(setFailUnfulfillableRequestInvokes.take()).isFalse();
        assertThat(setFailUnfulfillableRequestInvokes.poll(50L, TimeUnit.MILLISECONDS)).isNull();

        rm.close();
    }

    private TestingStandaloneResourceManager createResourceManager(
            Duration startupPeriod, SlotManager slotManager) throws Exception {

        final MockResourceManagerRuntimeServices rmServices =
                new MockResourceManagerRuntimeServices(
                        RPC_SERVICE_EXTENSION_WRAPPER.getCustomExtension().getTestingRpcService(),
                        slotManager);

        final TestingStandaloneResourceManager rm =
                new TestingStandaloneResourceManager(
                        rmServices.rpcService,
                        UUID.randomUUID(),
                        ResourceID.generate(),
                        rmServices.heartbeatServices,
                        rmServices.delegationTokenManager,
                        rmServices.slotManager,
                        rmServices.jobLeaderIdService,
                        new ClusterInformation("localhost", 1234),
                        fatalErrorHandler,
                        UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(),
                        startupPeriod);

        rm.start();
        rm.getStartedFuture().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        return rm;
    }

    private static class TestingStandaloneResourceManager extends StandaloneResourceManager {

        private TestingStandaloneResourceManager(
                RpcService rpcService,
                UUID leaderSessionId,
                ResourceID resourceId,
                HeartbeatServices heartbeatServices,
                DelegationTokenManager delegationTokenManager,
                SlotManager slotManager,
                JobLeaderIdService jobLeaderIdService,
                ClusterInformation clusterInformation,
                FatalErrorHandler fatalErrorHandler,
                ResourceManagerMetricGroup resourceManagerMetricGroup,
                Duration startupPeriodTime) {
            super(
                    rpcService,
                    leaderSessionId,
                    resourceId,
                    heartbeatServices,
                    delegationTokenManager,
                    slotManager,
                    NoOpResourceManagerPartitionTracker::get,
                    new NoOpBlocklistHandler.Factory(),
                    jobLeaderIdService,
                    clusterInformation,
                    fatalErrorHandler,
                    resourceManagerMetricGroup,
                    startupPeriodTime,
                    RpcUtils.INF_TIMEOUT,
                    ForkJoinPool.commonPool());
        }
    }
}
