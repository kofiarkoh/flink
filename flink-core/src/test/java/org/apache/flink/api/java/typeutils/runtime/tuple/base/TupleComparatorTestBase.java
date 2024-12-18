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

import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

public abstract @ExtendWith(CTestJUnit5Extension.class) @CTestClass class TupleComparatorTestBase<
                T extends Tuple>
        extends ComparatorTestBase<T> {

    @Override
    protected void deepEquals(String message, T should, T is) {
        for (int x = 0; x < should.getArity(); x++) {
            assertThat((Object) is.getField(x)).isEqualTo(should.getField(x));
        }
    }

    @Override
    protected abstract TupleComparator<T> createComparator(boolean ascending);

    @Override
    protected abstract TupleSerializer<T> createSerializer();
}
