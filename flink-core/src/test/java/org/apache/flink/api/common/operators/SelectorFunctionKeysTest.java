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

package org.apache.flink.api.common.operators;

import org.apache.flink.api.common.operators.ExpressionKeysTest.Pojo1;
import org.apache.flink.api.common.operators.ExpressionKeysTest.Pojo2;
import org.apache.flink.api.common.operators.ExpressionKeysTest.PojoWithMultiplePojos;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class SelectorFunctionKeysTest {

    @Test
    void testAreCompatible1() throws Keys.IncompatibleKeysException {
        TypeInformation<Pojo2> t1 = TypeExtractor.getForClass(Pojo2.class);
        TypeInformation<Tuple2<Integer, String>> t2 =
                new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        Keys<Pojo2> k1 =
                new Keys.SelectorFunctionKeys<>(
                        new KeySelector1(), t1, BasicTypeInfo.STRING_TYPE_INFO);
        Keys<Tuple2<Integer, String>> k2 =
                new Keys.SelectorFunctionKeys<>(
                        new KeySelector2(), t2, BasicTypeInfo.STRING_TYPE_INFO);

        assertThat(k1.areCompatible(k2)).isTrue();
        assertThat(k2.areCompatible(k1)).isTrue();
    }

    @Test
    void testAreCompatible2() throws Keys.IncompatibleKeysException {
        TypeInformation<PojoWithMultiplePojos> t1 =
                TypeExtractor.getForClass(PojoWithMultiplePojos.class);
        TypeInformation<Tuple3<Long, Pojo1, Integer>> t2 =
                new TupleTypeInfo<>(
                        BasicTypeInfo.LONG_TYPE_INFO,
                        TypeExtractor.getForClass(Pojo1.class),
                        BasicTypeInfo.INT_TYPE_INFO);
        TypeInformation<Tuple2<Integer, String>> kt =
                new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        Keys<PojoWithMultiplePojos> k1 =
                new Keys.SelectorFunctionKeys<>(new KeySelector3(), t1, kt);
        Keys<Tuple3<Long, Pojo1, Integer>> k2 =
                new Keys.SelectorFunctionKeys<>(new KeySelector4(), t2, kt);

        assertThat(k1.areCompatible(k2)).isTrue();
        assertThat(k2.areCompatible(k1)).isTrue();
    }

    @Test
    void testAreCompatible3() throws Keys.IncompatibleKeysException {
        TypeInformation<String> t1 = BasicTypeInfo.STRING_TYPE_INFO;
        TypeInformation<Pojo2> t2 = TypeExtractor.getForClass(Pojo2.class);

        Keys.ExpressionKeys<String> ek1 = new Keys.ExpressionKeys<>("*", t1);
        Keys<Pojo2> sk2 =
                new Keys.SelectorFunctionKeys<>(
                        new KeySelector1(), t2, BasicTypeInfo.STRING_TYPE_INFO);

        assertThat(sk2.areCompatible(ek1)).isTrue();
    }

    @Test
    void testAreCompatible4() throws Keys.IncompatibleKeysException {
        TypeInformation<Tuple3<String, Long, Integer>> t1 =
                new TupleTypeInfo<>(
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.LONG_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO);
        TypeInformation<PojoWithMultiplePojos> t2 =
                TypeExtractor.getForClass(PojoWithMultiplePojos.class);

        Keys.ExpressionKeys<Tuple3<String, Long, Integer>> ek1 =
                new Keys.ExpressionKeys<>(new int[] {2, 0}, t1);
        Keys<PojoWithMultiplePojos> sk2 =
                new Keys.SelectorFunctionKeys<>(
                        new KeySelector3(),
                        t2,
                        new TupleTypeInfo<>(
                                BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO));

        assertThat(sk2.areCompatible(ek1)).isTrue();
    }

    @Test
    void testOriginalTypes1() {
        TypeInformation<Tuple2<Integer, String>> t2 =
                new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        Keys<Tuple2<Integer, String>> k =
                new Keys.SelectorFunctionKeys<>(
                        new KeySelector2(), t2, BasicTypeInfo.STRING_TYPE_INFO);

        assertThat(k.getOriginalKeyFieldTypes()).containsExactly(BasicTypeInfo.STRING_TYPE_INFO);
    }

    @Test
    void testOriginalTypes2() {
        final TupleTypeInfo<Tuple2<Integer, String>> t1 =
                new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        TypeInformation<PojoWithMultiplePojos> t2 =
                TypeExtractor.getForClass(PojoWithMultiplePojos.class);

        Keys<PojoWithMultiplePojos> sk =
                new Keys.SelectorFunctionKeys<>(new KeySelector3(), t2, t1);

        assertThat(sk.getOriginalKeyFieldTypes()).isEqualTo(new TypeInformation<?>[] {t1});
    }

    public static class KeySelector1 implements KeySelector<Pojo2, String> {

        @Override
        public String getKey(Pojo2 v) {
            return v.b2;
        }
    }

    public static class KeySelector2 implements KeySelector<Tuple2<Integer, String>, String> {

        @Override
        public String getKey(Tuple2<Integer, String> v) {
            return v.f1;
        }
    }

    public static class KeySelector3
            implements KeySelector<PojoWithMultiplePojos, Tuple2<Integer, String>> {

        @Override
        public Tuple2<Integer, String> getKey(PojoWithMultiplePojos v) {
            return new Tuple2<>(v.i0, v.p1.b);
        }
    }

    public static class KeySelector4
            implements KeySelector<Tuple3<Long, Pojo1, Integer>, Tuple2<Integer, String>> {

        @Override
        public Tuple2<Integer, String> getKey(Tuple3<Long, Pojo1, Integer> v) {
            return new Tuple2<>(v.f2, v.f1.a);
        }
    }
}
