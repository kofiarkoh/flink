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

import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderelection.TestingLeaderElection;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** ResourceManager HA test, including grant leadership and revoke leadership. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class ResourceManagerHATest {

    @Test
    void testGrantAndRevokeLeadership() throws Exception {
        final TestingLeaderElection leaderElection = new TestingLeaderElection();

        final TestingResourceManagerService resourceManagerService =
                TestingResourceManagerService.newBuilder()
                        .setRmLeaderElection(leaderElection)
                        .build();

        try {
            resourceManagerService.start();

            final UUID leaderId = UUID.randomUUID();
            final LeaderInformation confirmedLeaderInformation =
                    resourceManagerService.isLeader(leaderId).join();

            // after grant leadership, verify resource manager is started with the fencing token
            assertThat(confirmedLeaderInformation.getLeaderSessionID()).isEqualTo(leaderId);
            assertThat(resourceManagerService.getResourceManagerFencingToken()).isPresent();
            assertThat(resourceManagerService.getResourceManagerFencingToken().get().toUUID())
                    .isEqualTo(leaderId);

            // then revoke leadership, verify resource manager is closed
            final Optional<CompletableFuture<Void>> rmTerminationFutureOpt =
                    resourceManagerService.getResourceManagerTerminationFuture();
            assertThat(rmTerminationFutureOpt).isPresent();

            resourceManagerService.notLeader();
            rmTerminationFutureOpt.get().get();

            resourceManagerService.rethrowFatalErrorIfAny();
        } finally {
            resourceManagerService.cleanUp();
        }
    }
}
