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
 * limitations under the License
 */

package org.apache.flink.runtime.scheduler.benchmark.failover;

import org.apache.flink.runtime.executiongraph.failover.RestartPipelinedRegionFailoverStrategy;
import org.apache.flink.runtime.scheduler.benchmark.JobConfiguration;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * The benchmark of calculating the regions to restart when failover occurs in a BATCH job. The
 * related method is {@link RestartPipelinedRegionFailoverStrategy#getTasksNeedingRestart}.
 */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class RegionToRestartInBatchJobBenchmarkTest {

    @Test
    void calculateRegionToRestart() throws Exception {
        RegionToRestartInBatchJobBenchmark benchmark = new RegionToRestartInBatchJobBenchmark();
        benchmark.setup(JobConfiguration.BATCH_TEST);
        benchmark.calculateRegionToRestart();
        benchmark.teardown();
    }
}
