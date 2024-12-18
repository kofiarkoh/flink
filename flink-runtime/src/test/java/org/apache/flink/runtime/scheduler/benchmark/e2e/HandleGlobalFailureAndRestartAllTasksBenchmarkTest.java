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

package org.apache.flink.runtime.scheduler.benchmark.e2e;

import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.benchmark.JobConfiguration;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * The benchmark of restarting tasks in a STREAMING/BATCH job. The related method is {@link
 * DefaultScheduler#handleGlobalFailure}.
 */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class HandleGlobalFailureAndRestartAllTasksBenchmarkTest {

    @ParameterizedTest
    @EnumSource(
            value = JobConfiguration.class,
            names = {"STREAMING_TEST", "BATCH_TEST", "STREAMING_EVENLY_TEST", "BATCH_EVENLY_TEST"})
    void deployAndRestarts(JobConfiguration jobConfiguration) throws Exception {
        HandleGlobalFailureAndRestartAllTasksBenchmark benchmark =
                new HandleGlobalFailureAndRestartAllTasksBenchmark();
        benchmark.setup(jobConfiguration);
        benchmark.handleGlobalFailureAndRestartAllTasks();
        benchmark.teardown();
    }
}
