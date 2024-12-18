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

package org.apache.flink.runtime.query;

import org.apache.flink.api.common.JobID;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KvStateLocation}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class KvStateLocationTest {

    /** Simple test registering/unregistering state and looking it up again. */
    @Test
    void testRegisterAndLookup() throws Exception {
        JobID jobId = new JobID();
        JobVertexID jobVertexId = new JobVertexID();
        int numKeyGroups = 123;
        int numRanges = 10;
        int fract = numKeyGroups / numRanges;
        int remain = numKeyGroups % numRanges;
        List<KeyGroupRange> keyGroupRanges = new ArrayList<>(numRanges);

        int start = 0;
        for (int i = 0; i < numRanges; ++i) {
            int end = start + fract - 1;
            if (remain > 0) {
                --remain;
                ++end;
            }
            KeyGroupRange range = new KeyGroupRange(start, end);
            keyGroupRanges.add(range);
            start = end + 1;
        }

        String registrationName = "asdasdasdasd";

        KvStateLocation location =
                new KvStateLocation(jobId, jobVertexId, numKeyGroups, registrationName);

        KvStateID[] kvStateIds = new KvStateID[numRanges];
        InetSocketAddress[] serverAddresses = new InetSocketAddress[numRanges];

        InetAddress host = InetAddress.getLocalHost();

        // Register
        int registeredCount = 0;
        for (int rangeIdx = 0; rangeIdx < numRanges; rangeIdx++) {
            kvStateIds[rangeIdx] = new KvStateID();
            serverAddresses[rangeIdx] = new InetSocketAddress(host, 1024 + rangeIdx);
            KeyGroupRange keyGroupRange = keyGroupRanges.get(rangeIdx);
            location.registerKvState(
                    keyGroupRange, kvStateIds[rangeIdx], serverAddresses[rangeIdx]);
            registeredCount += keyGroupRange.getNumberOfKeyGroups();
            assertThat(location.getNumRegisteredKeyGroups()).isEqualTo(registeredCount);
        }

        // Lookup
        for (int rangeIdx = 0; rangeIdx < numRanges; rangeIdx++) {
            KeyGroupRange keyGroupRange = keyGroupRanges.get(rangeIdx);
            for (int keyGroup = keyGroupRange.getStartKeyGroup();
                    keyGroup <= keyGroupRange.getEndKeyGroup();
                    ++keyGroup) {
                assertThat(location.getKvStateID(keyGroup)).isEqualTo(kvStateIds[rangeIdx]);
                assertThat(location.getKvStateServerAddress(keyGroup))
                        .isEqualTo(serverAddresses[rangeIdx]);
            }
        }

        // Overwrite
        for (int rangeIdx = 0; rangeIdx < numRanges; rangeIdx++) {
            kvStateIds[rangeIdx] = new KvStateID();
            serverAddresses[rangeIdx] = new InetSocketAddress(host, 1024 + rangeIdx);

            location.registerKvState(
                    keyGroupRanges.get(rangeIdx), kvStateIds[rangeIdx], serverAddresses[rangeIdx]);
            assertThat(location.getNumRegisteredKeyGroups()).isEqualTo(registeredCount);
        }

        // Lookup
        for (int rangeIdx = 0; rangeIdx < numRanges; rangeIdx++) {
            KeyGroupRange keyGroupRange = keyGroupRanges.get(rangeIdx);
            for (int keyGroup = keyGroupRange.getStartKeyGroup();
                    keyGroup <= keyGroupRange.getEndKeyGroup();
                    ++keyGroup) {
                assertThat(location.getKvStateID(keyGroup)).isEqualTo(kvStateIds[rangeIdx]);
                assertThat(location.getKvStateServerAddress(keyGroup))
                        .isEqualTo(serverAddresses[rangeIdx]);
            }
        }

        // Unregister
        for (int rangeIdx = 0; rangeIdx < numRanges; rangeIdx++) {
            KeyGroupRange keyGroupRange = keyGroupRanges.get(rangeIdx);
            location.unregisterKvState(keyGroupRange);
            registeredCount -= keyGroupRange.getNumberOfKeyGroups();
            assertThat(location.getNumRegisteredKeyGroups()).isEqualTo(registeredCount);
        }

        // Lookup
        for (int rangeIdx = 0; rangeIdx < numRanges; rangeIdx++) {
            KeyGroupRange keyGroupRange = keyGroupRanges.get(rangeIdx);
            for (int keyGroup = keyGroupRange.getStartKeyGroup();
                    keyGroup <= keyGroupRange.getEndKeyGroup();
                    ++keyGroup) {
                assertThat(location.getKvStateID(keyGroup)).isNull();
                assertThat(location.getKvStateServerAddress(keyGroup)).isNull();
            }
        }

        assertThat(location.getNumRegisteredKeyGroups()).isZero();
    }
}
