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

package org.apache.flink.state.api.utils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.test.util.AbstractTestBaseJUnit4;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.AbstractID;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.execution.ExecutionState.RUNNING;

/** A test base that includes utilities for taking a savepoint. */
public abstract @ExtendWith(CTestJUnit5Extension.class) @CTestClass class SavepointTestBase
        extends AbstractTestBaseJUnit4 {

    public String takeSavepoint(StreamExecutionEnvironment executionEnvironment) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableClosureCleaner();

        JobGraph jobGraph = executionEnvironment.getStreamGraph().getJobGraph();

        JobID jobId = jobGraph.getJobID();

        ClusterClient<?> client = MINI_CLUSTER_RESOURCE.getClusterClient();

        try {
            JobID jobID = client.submitJob(jobGraph).get();

            waitForAllRunningOrSomeTerminal(jobID, MINI_CLUSTER_RESOURCE);

            return triggerSavepoint(client, jobID).get(5, TimeUnit.MINUTES);
        } catch (Exception e) {
            throw new RuntimeException("Failed to take savepoint", e);
        } finally {
            client.cancel(jobId);
        }
    }

    public static void waitForAllRunningOrSomeTerminal(
            JobID jobID, MiniClusterWithClientResource miniClusterResource) throws Exception {
        while (true) {
            JobDetailsInfo jobInfo =
                    miniClusterResource.getRestClusterClient().getJobDetails(jobID).get();
            Set<ExecutionState> vertexStates =
                    jobInfo.getJobVertexInfos().stream()
                            .map(JobDetailsInfo.JobVertexDetailsInfo::getExecutionState)
                            .collect(Collectors.toSet());
            if (vertexStates.equals(EnumSet.of(RUNNING))
                    || vertexStates.stream().anyMatch(ExecutionState::isTerminal)) {
                return;
            } else {
                Thread.sleep(500);
            }
        }
    }

    public <T> SourceFunction<T> createSource(T[] data) {
        return createSource(Arrays.asList(data));
    }

    public <T> SourceFunction<T> createSource(Collection<T> data) {
        T first = data.iterator().next();
        if (first == null) {
            throw new IllegalArgumentException("Collection must not contain null elements");
        }

        TypeInformation<T> typeInfo = TypeExtractor.getForObject(first);
        try {
            SourceFunction<T> inner =
                    new FromElementsFunction<>(
                            typeInfo.createSerializer(new SerializerConfigImpl()), data);
            return new WaitingSource<>(inner, typeInfo);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private CompletableFuture<String> triggerSavepoint(ClusterClient<?> client, JobID jobID)
            throws RuntimeException {
        try {
            String dirPath = getTempDirPath(new AbstractID().toHexString());
            return client.triggerSavepoint(jobID, dirPath, SavepointFormatType.CANONICAL);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
