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

package org.apache.flink.runtime.checkpoint;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class StatsSummaryTest {

    /** Test the initial/empty state. */
    @Test
    void testInitialState() {
        StatsSummary mma = new StatsSummary();

        assertThat(mma.getMinimum()).isZero();
        assertThat(mma.getMaximum()).isZero();
        assertThat(mma.getSum()).isZero();
        assertThat(mma.getCount()).isZero();
        assertThat(mma.getAverage()).isZero();
    }

    /** Test that non-positive numbers are not counted. */
    @Test
    void testAddNonPositiveStats() {
        StatsSummary mma = new StatsSummary();
        mma.add(-1);

        assertThat(mma.getMinimum()).isZero();
        assertThat(mma.getMaximum()).isZero();
        assertThat(mma.getSum()).isZero();
        assertThat(mma.getCount()).isZero();
        assertThat(mma.getAverage()).isZero();

        mma.add(0);

        assertThat(mma.getMinimum()).isZero();
        assertThat(mma.getMaximum()).isZero();
        assertThat(mma.getSum()).isZero();
        assertThat(mma.getCount()).isOne();
        assertThat(mma.getAverage()).isZero();
    }

    /** Test sequence of random numbers. */
    @Test
    void testAddRandomNumbers() {
        ThreadLocalRandom rand = ThreadLocalRandom.current();

        StatsSummary mma = new StatsSummary();

        long count = 13;
        long sum = 0;
        long min = Integer.MAX_VALUE;
        long max = Integer.MIN_VALUE;

        for (int i = 0; i < count; i++) {
            int number = rand.nextInt(124) + 1;
            sum += number;
            min = Math.min(min, number);
            max = Math.max(max, number);

            mma.add(number);
        }

        assertThat(mma.getMinimum()).isEqualTo(min);
        assertThat(mma.getMaximum()).isEqualTo(max);
        assertThat(mma.getSum()).isEqualTo(sum);
        assertThat(mma.getCount()).isEqualTo(count);
        assertThat(mma.getAverage()).isEqualTo(sum / count);
    }

    @Test
    void testQuantile() {
        StatsSummary summary = new StatsSummary(100);
        for (int i = 0; i < 123; i++) {
            summary.add(100000); // should be forced out by the later values
        }
        for (int i = 1; i <= 100; i++) {
            summary.add(i);
        }
        StatsSummarySnapshot snapshot = summary.createSnapshot();
        for (double q = 0.01; q <= 1; q++) {
            assertThat(snapshot.getQuantile(q)).isCloseTo(q, offset(1d));
        }
    }
}
