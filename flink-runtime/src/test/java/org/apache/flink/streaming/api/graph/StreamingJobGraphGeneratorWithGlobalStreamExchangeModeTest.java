/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link StreamingJobGraphGenerator} on different {@link GlobalStreamExchangeMode}
 * settings.
 */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class StreamingJobGraphGeneratorWithGlobalStreamExchangeModeTest {

    @Test
    void testDefaultGlobalExchangeModeIsAllEdgesPipelined() {
        final StreamGraph streamGraph = createStreamGraph();
        assertThat(streamGraph.getGlobalStreamExchangeMode())
                .isEqualTo(GlobalStreamExchangeMode.ALL_EDGES_PIPELINED);
    }

    @Test
    void testAllEdgesBlockingMode() {
        final StreamGraph streamGraph =
                createStreamGraph(GlobalStreamExchangeMode.ALL_EDGES_BLOCKING);
        final JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);

        final List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
        final JobVertex sourceVertex = verticesSorted.get(0);
        final JobVertex map1Vertex = verticesSorted.get(1);
        final JobVertex map2Vertex = verticesSorted.get(2);

        assertThat(sourceVertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.BLOCKING);
        assertThat(map1Vertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.BLOCKING);
        assertThat(map2Vertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.BLOCKING);
    }

    @Test
    void testAllEdgesPipelinedMode() {
        final StreamGraph streamGraph = createStreamGraph();
        streamGraph.setGlobalStreamExchangeMode(GlobalStreamExchangeMode.ALL_EDGES_PIPELINED);
        final JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);

        final List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
        final JobVertex sourceVertex = verticesSorted.get(0);
        final JobVertex map1Vertex = verticesSorted.get(1);
        final JobVertex map2Vertex = verticesSorted.get(2);

        assertThat(sourceVertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.PIPELINED_BOUNDED);
        assertThat(map1Vertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.PIPELINED_BOUNDED);
        assertThat(map2Vertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.PIPELINED_BOUNDED);
    }

    @Test
    void testForwardEdgesPipelinedMode() {
        final StreamGraph streamGraph =
                createStreamGraph(GlobalStreamExchangeMode.FORWARD_EDGES_PIPELINED);
        final JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);

        final List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
        final JobVertex sourceVertex = verticesSorted.get(0);
        final JobVertex map1Vertex = verticesSorted.get(1);
        final JobVertex map2Vertex = verticesSorted.get(2);

        assertThat(sourceVertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.PIPELINED_BOUNDED);
        assertThat(map1Vertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.BLOCKING);
        assertThat(map2Vertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.BLOCKING);
    }

    @Test
    void testPointwiseEdgesPipelinedMode() {
        final StreamGraph streamGraph =
                createStreamGraph(GlobalStreamExchangeMode.POINTWISE_EDGES_PIPELINED);
        final JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);

        final List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
        final JobVertex sourceVertex = verticesSorted.get(0);
        final JobVertex map1Vertex = verticesSorted.get(1);
        final JobVertex map2Vertex = verticesSorted.get(2);

        assertThat(sourceVertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.PIPELINED_BOUNDED);
        assertThat(map1Vertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.PIPELINED_BOUNDED);
        assertThat(map2Vertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.BLOCKING);
    }

    @Test
    void testGlobalExchangeModeDoesNotOverrideSpecifiedExchangeMode() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<Integer> source = env.fromData(1, 2, 3).setParallelism(1);
        final DataStream<Integer> forward =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                source.getTransformation(),
                                new ForwardPartitioner<>(),
                                StreamExchangeMode.PIPELINED));
        forward.map(i -> i).startNewChain().setParallelism(1);
        final StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setGlobalStreamExchangeMode(GlobalStreamExchangeMode.ALL_EDGES_BLOCKING);

        final JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);

        final List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
        final JobVertex sourceVertex = verticesSorted.get(0);

        assertThat(sourceVertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.PIPELINED_BOUNDED);
    }

    /**
     * Topology: source(parallelism=1) --(forward)--> map1(parallelism=1) --(rescale)-->
     * map2(parallelism=2) --(rebalance)--> sink(parallelism=2).
     */
    private static StreamGraph createStreamGraph() {
        return createStreamGraph(GlobalStreamExchangeMode.ALL_EDGES_PIPELINED);
    }

    /**
     * Topology: source(parallelism=1) --(forward)--> map1(parallelism=1) --(rescale)-->
     * map2(parallelism=2) --(rebalance)--> sink(parallelism=2).
     */
    private static StreamGraph createStreamGraph(
            GlobalStreamExchangeMode globalStreamExchangeMode) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (globalStreamExchangeMode != GlobalStreamExchangeMode.ALL_EDGES_PIPELINED) {
            env.setBufferTimeout(-1);
        }

        final DataStream<Integer> source = env.fromData(1, 2, 3).setParallelism(1);

        final DataStream<Integer> forward =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                source.getTransformation(),
                                new ForwardPartitioner<>(),
                                StreamExchangeMode.UNDEFINED));
        final DataStream<Integer> map1 = forward.map(i -> i).startNewChain().setParallelism(1);

        final DataStream<Integer> rescale =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                map1.getTransformation(),
                                new RescalePartitioner<>(),
                                StreamExchangeMode.UNDEFINED));
        final DataStream<Integer> map2 = rescale.map(i -> i).setParallelism(2);

        map2.rebalance().print().setParallelism(2);

        final StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setGlobalStreamExchangeMode(globalStreamExchangeMode);
        return streamGraph;
    }
}
