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

package org.apache.flink.util;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link LongValueSequenceIterator}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class LongValueSequenceIteratorTest {

    @Test
    void testSplitRegular() {
        testSplitting(new org.apache.flink.util.LongValueSequenceIterator(0, 10), 2);
        testSplitting(new org.apache.flink.util.LongValueSequenceIterator(100, 100000), 7);
        testSplitting(new org.apache.flink.util.LongValueSequenceIterator(-100, 0), 5);
        testSplitting(new org.apache.flink.util.LongValueSequenceIterator(-100, 100), 3);
    }

    @Test
    void testSplittingLargeRangesBy2() {
        testSplitting(new org.apache.flink.util.LongValueSequenceIterator(0, Long.MAX_VALUE), 2);
        testSplitting(
                new org.apache.flink.util.LongValueSequenceIterator(-1000000000L, Long.MAX_VALUE),
                2);
        testSplitting(
                new org.apache.flink.util.LongValueSequenceIterator(Long.MIN_VALUE, Long.MAX_VALUE),
                2);
    }

    @Test
    void testSplittingTooSmallRanges() {
        testSplitting(new org.apache.flink.util.LongValueSequenceIterator(0, 0), 2);
        testSplitting(new org.apache.flink.util.LongValueSequenceIterator(-5, -5), 2);
        testSplitting(new org.apache.flink.util.LongValueSequenceIterator(-5, -4), 3);
        testSplitting(new org.apache.flink.util.LongValueSequenceIterator(10, 15), 10);
    }

    private static void testSplitting(
            org.apache.flink.util.LongValueSequenceIterator iter, int numSplits) {
        org.apache.flink.util.LongValueSequenceIterator[] splits = iter.split(numSplits);

        assertThat(splits).hasSize(numSplits);

        // test start and end of range
        assertThat(splits[0].getCurrent()).isEqualTo(iter.getCurrent());
        assertThat(splits[numSplits - 1].getTo()).isEqualTo(iter.getTo());

        // test continuous range
        for (int i = 1; i < splits.length; i++) {
            assertThat(splits[i].getCurrent()).isEqualTo(splits[i - 1].getTo() + 1);
        }

        testMaxSplitDiff(splits);
    }

    private static void testMaxSplitDiff(org.apache.flink.util.LongValueSequenceIterator[] iters) {
        long minSplitSize = Long.MAX_VALUE;
        long maxSplitSize = Long.MIN_VALUE;

        for (LongValueSequenceIterator iter : iters) {
            long diff;
            if (iter.getTo() < iter.getCurrent()) {
                diff = 0;
            } else {
                diff = iter.getTo() - iter.getCurrent();
            }
            if (diff < 0) {
                diff = Long.MAX_VALUE;
            }

            minSplitSize = Math.min(minSplitSize, diff);
            maxSplitSize = Math.max(maxSplitSize, diff);
        }

        assertThat(maxSplitSize == minSplitSize || maxSplitSize - 1 == minSplitSize).isTrue();
    }
}
