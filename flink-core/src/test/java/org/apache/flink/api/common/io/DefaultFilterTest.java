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
package org.apache.flink.api.common.io;

import org.apache.flink.core.fs.Path;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ParameterizedTestExtension.class)
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class DefaultFilterTest {
    @Parameters
    private static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    {"file.txt", false},
                    {".file.txt", true},
                    {"dir/.file.txt", true},
                    {".dir/file.txt", false},
                    {"_file.txt", true},
                    {"dir/_file.txt", true},
                    {"_dir/file.txt", false},

                    // Check filtering Hadoop's unfinished files
                    {FilePathFilter.HADOOP_COPYING, true},
                    {"dir/" + FilePathFilter.HADOOP_COPYING, true},
                    {FilePathFilter.HADOOP_COPYING + "/file.txt", false},
                });
    }

    private final boolean shouldFilter;
    private final String filePath;

    DefaultFilterTest(String filePath, boolean shouldFilter) {
        this.filePath = filePath;
        this.shouldFilter = shouldFilter;
    }

    @TestTemplate
    void test() {
        FilePathFilter defaultFilter = FilePathFilter.createDefaultFilter();
        Path path = new Path(filePath);
        assertThat(defaultFilter.filterPath(path)).as("File: " + filePath).isEqualTo(shouldFilter);
    }
}
