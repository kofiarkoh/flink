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

package org.apache.flink.api.java.typeutils.runtime.tuple.base;

import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Abstract test base for TuplePairComparators.
 *
 * @param <T>
 * @param <R>
 */
public abstract @ExtendWith(CTestJUnit5Extension.class) @CTestClass
class TuplePairComparatorTestBase<T extends Tuple, R extends Tuple> {

    protected abstract TypePairComparator<T, R> createComparator(boolean ascending);

    protected abstract Tuple2<T[], R[]> getSortedTestData();

    @Test
    void testEqualityWithReference() {
        TypePairComparator<T, R> comparator = getComparator(true);
        Tuple2<T[], R[]> data = getSortedData();
        for (int x = 0; x < data.f0.length; x++) {
            comparator.setReference(data.f0[x]);

            assertThat(comparator.equalToReference(data.f1[x])).isTrue();
        }
    }

    @Test
    void testInequalityWithReference() {
        testGreatSmallAscDescWithReference(true);
        testGreatSmallAscDescWithReference(false);
    }

    protected void testGreatSmallAscDescWithReference(boolean ascending) {
        Tuple2<T[], R[]> data = getSortedData();

        TypePairComparator<T, R> comparator = getComparator(ascending);

        // compares every element in high with every element in low
        for (int x = 0; x < data.f0.length - 1; x++) {
            for (int y = x + 1; y < data.f1.length; y++) {
                comparator.setReference(data.f0[x]);
                if (ascending) {
                    assertThat(comparator.compareToReference(data.f1[y])).isPositive();
                } else {
                    assertThat(comparator.compareToReference(data.f1[y])).isNegative();
                }
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    protected TypePairComparator<T, R> getComparator(boolean ascending) {
        TypePairComparator<T, R> comparator = createComparator(ascending);
        if (comparator == null) {
            throw new RuntimeException("Test case corrupt. Returns null as comparator.");
        }
        return comparator;
    }

    protected Tuple2<T[], R[]> getSortedData() {
        Tuple2<T[], R[]> data = getSortedTestData();
        if (data == null || data.f0 == null || data.f1 == null) {
            throw new RuntimeException("Test case corrupt. Returns null as test data.");
        }
        if (data.f0.length < 2 || data.f1.length < 2) {
            throw new RuntimeException("Test case does not provide enough sorted test data.");
        }

        return data;
    }
}
