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

package org.apache.flink.runtime.metrics.utils;

import org.apache.flink.runtime.metrics.util.SystemResourcesCounter;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/** Tests for {@link SystemResourcesCounter}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class SystemResourcesCounterTest {

    private static final double EPSILON = 0.01;

    @Test
    void testObtainAnyMetrics() throws InterruptedException {
        SystemResourcesCounter systemResources = new SystemResourcesCounter(Duration.ofMillis(10));
        double initialCpuIdle = systemResources.getCpuIdle();

        systemResources.start();
        // wait for stats to update/calculate
        try {
            double cpuIdle;
            do {
                Thread.sleep(1);
                cpuIdle = systemResources.getCpuIdle();
            } while (systemResources.isAlive()
                    && (initialCpuIdle == cpuIdle || Double.isNaN(cpuIdle) || cpuIdle == 0.0));
        } finally {
            systemResources.shutdown();
            systemResources.join();
        }

        double totalCpuUsage =
                systemResources.getCpuIrq()
                        + systemResources.getCpuNice()
                        + systemResources.getCpuSoftIrq()
                        + systemResources.getCpuSys()
                        + systemResources.getCpuUser()
                        + systemResources.getIOWait()
                        + systemResources.getCpuSteal();

        assertThat(systemResources.getProcessorsCount())
                .withFailMessage("There should be at least one processor")
                .isGreaterThan(0);
        assertThat(systemResources.getNetworkInterfaceNames().length)
                .withFailMessage("There should be at least one network interface")
                .isGreaterThan(0);
        assertThat(totalCpuUsage + systemResources.getCpuIdle()).isCloseTo(100.0, within(EPSILON));
    }
}
