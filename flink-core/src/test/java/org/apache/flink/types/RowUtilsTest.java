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

package org.apache.flink.types;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowUtils}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class RowUtilsTest {

    @Test
    void testCompareRowsUnordered() {
        final List<Row> originalList =
                Arrays.asList(
                        Row.of("a", 12, false),
                        Row.of("b", 12, false),
                        Row.of("b", 12, false),
                        Row.of("b", 12, true));

        {
            final List<Row> list =
                    Arrays.asList(
                            Row.of("a", 12, false),
                            Row.of("b", 12, false),
                            Row.of("b", 12, false),
                            Row.of("b", 12, true));
            assertThat(RowUtils.compareRows(originalList, list, false)).isTrue();
        }

        {
            final List<Row> list =
                    Arrays.asList(
                            Row.of("a", 12, false),
                            Row.of("b", 12, false),
                            Row.of("b", 12, true), // diff order here
                            Row.of("b", 12, false));
            assertThat(RowUtils.compareRows(originalList, list, false)).isFalse();
        }

        {
            final List<Row> list =
                    Arrays.asList(
                            Row.of("a", 12, false),
                            Row.of("b", 12, false),
                            Row.of("b", 12, true), // diff order here
                            Row.of("b", 12, false));
            assertThat(RowUtils.compareRows(originalList, list, true)).isTrue();
        }

        {
            final List<Row> list =
                    Arrays.asList(
                            Row.of("a", 12, false),
                            Row.of("b", 12, false),
                            Row.of("b", 12, false),
                            Row.of("b", 12, true),
                            Row.of("b", 12, true)); // diff here
            assertThat(RowUtils.compareRows(originalList, list, true)).isFalse();
        }
    }
}
