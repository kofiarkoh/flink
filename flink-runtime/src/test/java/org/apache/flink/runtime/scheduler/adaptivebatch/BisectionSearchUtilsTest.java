/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler.adaptivebatch;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BisectionSearchUtils}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class BisectionSearchUtilsTest {

    @Test
    void testFindMinLegalValue() {
        assertThat(BisectionSearchUtils.findMinLegalValue(value -> 29 / value <= 3, 1, 17))
                .isEqualTo(8);
        assertThat(BisectionSearchUtils.findMinLegalValue(value -> 29 / value <= 3, 8, 17))
                .isEqualTo(8);
        assertThat(BisectionSearchUtils.findMinLegalValue(value -> 29 / value <= 3, 1, 8))
                .isEqualTo(8);
        assertThat(BisectionSearchUtils.findMinLegalValue(value -> 29 / value <= 3, 9, 17))
                .isEqualTo(9);
        assertThat(BisectionSearchUtils.findMinLegalValue(value -> 29 / value <= 3, 1, 7))
                .isEqualTo(-1);
    }

    @Test
    void testFindMaxLegalValue() {
        assertThat(BisectionSearchUtils.findMaxLegalValue(value -> value / 3 <= 3, 1, 17))
                .isEqualTo(11);
        assertThat(BisectionSearchUtils.findMaxLegalValue(value -> value / 3 <= 3, 11, 17))
                .isEqualTo(11);
        assertThat(BisectionSearchUtils.findMaxLegalValue(value -> value / 3 <= 3, 1, 11))
                .isEqualTo(11);
        assertThat(BisectionSearchUtils.findMaxLegalValue(value -> value / 3 <= 3, 1, 10))
                .isEqualTo(10);
        assertThat(BisectionSearchUtils.findMaxLegalValue(value -> value / 3 <= 3, 12, 17))
                .isEqualTo(-1);
    }
}
