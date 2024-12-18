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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.ComparatorTestBase.TestInputView;
import org.apache.flink.api.common.typeutils.ComparatorTestBase.TestOutputView;
import org.apache.flink.api.common.typeutils.SerializerTestInstance;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.Either;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import static org.apache.flink.types.Either.Left;
import static org.apache.flink.types.Either.Right;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class EitherSerializerTest {

    @SuppressWarnings("unchecked")
    @Test
    void testStringDoubleEither() {

        Either<String, Double>[] testData =
                new Either[] {
                    Left("banana"),
                    Left(""),
                    Right(32.0),
                    Right(Double.MIN_VALUE),
                    Right(Double.MAX_VALUE)
                };

        EitherTypeInfo<String, Double> eitherTypeInfo =
                new EitherTypeInfo<String, Double>(
                        BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO);
        EitherSerializer<String, Double> eitherSerializer =
                (EitherSerializer<String, Double>)
                        eitherTypeInfo.createSerializer(new SerializerConfigImpl());
        SerializerTestInstance<Either<String, Double>> testInstance =
                new EitherSerializerTestInstance<Either<String, Double>>(
                        eitherSerializer, eitherTypeInfo.getTypeClass(), -1, testData);
        testInstance.testAll();
    }

    @Test
    void testStringValueDoubleValueEither() {
        @SuppressWarnings("unchecked")
        Either<StringValue, DoubleValue>[] testData =
                new Either[] {
                    Left(new StringValue("banana")),
                    Left.of(new StringValue("apple")),
                    new Left(new StringValue("")),
                    Right(new DoubleValue(32.0)),
                    Right.of(new DoubleValue(Double.MIN_VALUE)),
                    new Right(new DoubleValue(Double.MAX_VALUE))
                };

        EitherTypeInfo<StringValue, DoubleValue> eitherTypeInfo =
                new EitherTypeInfo<>(
                        ValueTypeInfo.STRING_VALUE_TYPE_INFO, ValueTypeInfo.DOUBLE_VALUE_TYPE_INFO);
        EitherSerializer<StringValue, DoubleValue> eitherSerializer =
                (EitherSerializer<StringValue, DoubleValue>)
                        eitherTypeInfo.createSerializer(new SerializerConfigImpl());
        SerializerTestInstance<Either<StringValue, DoubleValue>> testInstance =
                new EitherSerializerTestInstance<>(
                        eitherSerializer, eitherTypeInfo.getTypeClass(), -1, testData);
        testInstance.testAll();
    }

    @SuppressWarnings("unchecked")
    @Test
    void testEitherWithTuple() {

        Either<Tuple2<Long, Long>, Double>[] testData =
                new Either[] {
                    Either.Left(new Tuple2<>(2L, 9L)),
                    new Left<>(new Tuple2<>(Long.MIN_VALUE, Long.MAX_VALUE)),
                    new Right<>(32.0),
                    Right(Double.MIN_VALUE),
                    Right(Double.MAX_VALUE)
                };

        EitherTypeInfo<Tuple2<Long, Long>, Double> eitherTypeInfo =
                new EitherTypeInfo<Tuple2<Long, Long>, Double>(
                        new TupleTypeInfo<Tuple2<Long, Long>>(
                                BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO),
                        BasicTypeInfo.DOUBLE_TYPE_INFO);
        EitherSerializer<Tuple2<Long, Long>, Double> eitherSerializer =
                (EitherSerializer<Tuple2<Long, Long>, Double>)
                        eitherTypeInfo.createSerializer(new SerializerConfigImpl());
        SerializerTestInstance<Either<Tuple2<Long, Long>, Double>> testInstance =
                new EitherSerializerTestInstance<Either<Tuple2<Long, Long>, Double>>(
                        eitherSerializer, eitherTypeInfo.getTypeClass(), -1, testData);
        testInstance.testAll();
    }

    @Test
    void testEitherWithTupleValues() {
        @SuppressWarnings("unchecked")
        Either<Tuple2<LongValue, LongValue>, DoubleValue>[] testData =
                new Either[] {
                    Left(new Tuple2<>(new LongValue(2L), new LongValue(9L))),
                    new Left<>(
                            new Tuple2<>(
                                    new LongValue(Long.MIN_VALUE), new LongValue(Long.MAX_VALUE))),
                    new Right<>(new DoubleValue(32.0)),
                    Right(new DoubleValue(Double.MIN_VALUE)),
                    Right(new DoubleValue(Double.MAX_VALUE))
                };

        EitherTypeInfo<Tuple2<LongValue, LongValue>, DoubleValue> eitherTypeInfo =
                new EitherTypeInfo<>(
                        new TupleTypeInfo<Tuple2<LongValue, LongValue>>(
                                ValueTypeInfo.LONG_VALUE_TYPE_INFO,
                                ValueTypeInfo.LONG_VALUE_TYPE_INFO),
                        ValueTypeInfo.DOUBLE_VALUE_TYPE_INFO);
        EitherSerializer<Tuple2<LongValue, LongValue>, DoubleValue> eitherSerializer =
                (EitherSerializer<Tuple2<LongValue, LongValue>, DoubleValue>)
                        eitherTypeInfo.createSerializer(new SerializerConfigImpl());
        SerializerTestInstance<Either<Tuple2<LongValue, LongValue>, DoubleValue>> testInstance =
                new EitherSerializerTestInstance<>(
                        eitherSerializer, eitherTypeInfo.getTypeClass(), -1, testData);
        testInstance.testAll();
    }

    @Test
    void testEitherWithObjectReuse() {
        EitherTypeInfo<LongValue, DoubleValue> eitherTypeInfo =
                new EitherTypeInfo<>(
                        ValueTypeInfo.LONG_VALUE_TYPE_INFO, ValueTypeInfo.DOUBLE_VALUE_TYPE_INFO);
        EitherSerializer<LongValue, DoubleValue> eitherSerializer =
                (EitherSerializer<LongValue, DoubleValue>)
                        eitherTypeInfo.createSerializer(new SerializerConfigImpl());

        LongValue lv = new LongValue();
        DoubleValue dv = new DoubleValue();

        Either<LongValue, DoubleValue> left = Left(lv);
        Either<LongValue, DoubleValue> right = Right(dv);

        // the first copy creates a new instance of Left
        Either<LongValue, DoubleValue> copy0 = eitherSerializer.copy(left, right);

        // then the cross-references are used for future copies
        Either<LongValue, DoubleValue> copy1 = eitherSerializer.copy(right, copy0);
        Either<LongValue, DoubleValue> copy2 = eitherSerializer.copy(left, copy1);

        // validate reference equality
        assertThat(copy1).isSameAs(right);
        assertThat(copy2).isSameAs(copy0);

        // validate reference equality of contained objects
        assertThat(copy1.right()).isSameAs(right.right());
        assertThat(copy2.left()).isSameAs(copy0.left());
    }

    @Test
    void testSerializeIndividually() throws IOException {
        EitherTypeInfo<LongValue, DoubleValue> eitherTypeInfo =
                new EitherTypeInfo<>(
                        ValueTypeInfo.LONG_VALUE_TYPE_INFO, ValueTypeInfo.DOUBLE_VALUE_TYPE_INFO);
        EitherSerializer<LongValue, DoubleValue> eitherSerializer =
                (EitherSerializer<LongValue, DoubleValue>)
                        eitherTypeInfo.createSerializer(new SerializerConfigImpl());

        LongValue lv = new LongValue();
        DoubleValue dv = new DoubleValue();

        Either<LongValue, DoubleValue> left = Left(lv);
        Either<LongValue, DoubleValue> right = Right(dv);

        TestOutputView out = new TestOutputView();
        eitherSerializer.serialize(left, out);
        eitherSerializer.serialize(right, out);
        eitherSerializer.serialize(left, out);

        TestInputView in = out.getInputView();
        // the first deserialization creates a new instance of Left
        Either<LongValue, DoubleValue> copy0 = eitherSerializer.deserialize(right, in);

        // then the cross-references are used for future copies
        Either<LongValue, DoubleValue> copy1 = eitherSerializer.deserialize(copy0, in);
        Either<LongValue, DoubleValue> copy2 = eitherSerializer.deserialize(copy1, in);

        // validate reference equality
        assertThat(copy1).isSameAs(right);
        assertThat(copy2).isSameAs(copy0);

        // validate reference equality of contained objects
        assertThat(copy1.right()).isSameAs(right.right());
        assertThat(copy2.left()).isSameAs(copy0.left());
    }

    /**
     * {@link org.apache.flink.api.common.typeutils.SerializerTestBase#testInstantiate()} checks
     * that the type of the created instance is the same as the type class parameter. Since we
     * arbitrarily create always create a Left instance we override this test.
     */
    @Nested
    private @ExtendWith(CTestJUnit5Extension.class) @CTestClass class EitherSerializerTestInstance<
                    T>
            extends SerializerTestInstance<T> {

        public EitherSerializerTestInstance(
                TypeSerializer<T> serializer, Class<T> typeClass, int length, T[] testData) {
            super(serializer, typeClass, length, testData);
        }

        @Override
        @Test
        protected void testInstantiate() {
            TypeSerializer<T> serializer = getSerializer();

            T instance = serializer.createInstance();
            assertThat(instance).as("The created instance must not be null.").isNotNull();

            Class<T> type = getTypeClass();
            assertThat(type).as("The test is corrupt: type class is null.").isNotNull();
        }
    }
}
