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

package org.apache.flink.runtime.util.config.memory;

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.api.common.resources.ExternalResource;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TaskExecutorProcessSpec}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class TaskExecutorProcessSpecTest {
    private static final String EXTERNAL_RESOURCE_NAME = "gpu";

    @Test
    void testEquals() {
        TaskExecutorProcessSpec spec1 =
                new TaskExecutorProcessSpec(
                        new CPUResource(1.0),
                        MemorySize.parse("1m"),
                        MemorySize.parse("2m"),
                        MemorySize.parse("3m"),
                        MemorySize.parse("4m"),
                        MemorySize.parse("5m"),
                        MemorySize.parse("6m"),
                        MemorySize.parse("7m"),
                        MemorySize.parse("8m"),
                        Collections.singleton(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1)));

        TaskExecutorProcessSpec spec2 =
                new TaskExecutorProcessSpec(
                        new CPUResource(1.0),
                        MemorySize.parse("1m"),
                        MemorySize.parse("2m"),
                        MemorySize.parse("3m"),
                        MemorySize.parse("4m"),
                        MemorySize.parse("5m"),
                        MemorySize.parse("6m"),
                        MemorySize.parse("7m"),
                        MemorySize.parse("8m"),
                        Collections.singleton(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1)));

        assertThat(spec1).isEqualTo(spec2);
    }

    @Test
    void testNotEquals() {
        TaskExecutorProcessSpec spec1 =
                new TaskExecutorProcessSpec(
                        new CPUResource(1.0),
                        MemorySize.parse("1m"),
                        MemorySize.parse("2m"),
                        MemorySize.parse("3m"),
                        MemorySize.parse("4m"),
                        MemorySize.parse("5m"),
                        MemorySize.parse("6m"),
                        MemorySize.parse("7m"),
                        MemorySize.parse("8m"),
                        Collections.singleton(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1)));

        TaskExecutorProcessSpec spec2 =
                new TaskExecutorProcessSpec(
                        new CPUResource(0.0),
                        MemorySize.ZERO,
                        MemorySize.ZERO,
                        MemorySize.ZERO,
                        MemorySize.ZERO,
                        MemorySize.ZERO,
                        MemorySize.ZERO,
                        MemorySize.ZERO,
                        MemorySize.ZERO,
                        Collections.emptyList());

        assertThat(spec1).isNotEqualTo(spec2);
    }
}
