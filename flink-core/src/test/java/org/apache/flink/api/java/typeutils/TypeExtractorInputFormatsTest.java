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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("serial")
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class TypeExtractorInputFormatsTest {

    @Test
    void testExtractInputFormatType() {
        InputFormat<?, ?> format = new DummyFloatInputFormat();
        TypeInformation<?> typeInfo = TypeExtractor.getInputFormatTypes(format);
        assertThat(typeInfo).isEqualTo(BasicTypeInfo.FLOAT_TYPE_INFO);
    }

    @Test
    void testExtractDerivedInputFormatType() {
        // simple type
        {
            InputFormat<?, ?> format = new DerivedInputFormat();
            TypeInformation<?> typeInfo = TypeExtractor.getInputFormatTypes(format);
            assertThat(typeInfo).isEqualTo(BasicTypeInfo.SHORT_TYPE_INFO);
        }

        // composite type
        {
            InputFormat<?, ?> format = new DerivedTupleInputFormat();
            TypeInformation<?> typeInfo = TypeExtractor.getInputFormatTypes(format);

            assertThat(typeInfo.isTupleType()).isTrue();
            assertThat(typeInfo).isInstanceOf(TupleTypeInfo.class);

            @SuppressWarnings("unchecked")
            TupleTypeInfo<Tuple3<String, Short, Double>> tupleInfo =
                    (TupleTypeInfo<Tuple3<String, Short, Double>>) typeInfo;

            assertThat(tupleInfo.getArity()).isEqualTo(3);
            assertThat(tupleInfo.getTypeAt(0)).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
            assertThat(tupleInfo.getTypeAt(1)).isEqualTo(BasicTypeInfo.SHORT_TYPE_INFO);
            assertThat(tupleInfo.getTypeAt(2)).isEqualTo(BasicTypeInfo.DOUBLE_TYPE_INFO);
        }
    }

    @Test
    void testMultiLevelDerivedInputFormatType() {

        // composite type
        InputFormat<?, ?> format = new FinalRelativeInputFormat();
        TypeInformation<?> typeInfo = TypeExtractor.getInputFormatTypes(format);

        assertThat(typeInfo.isTupleType()).isTrue();
        assertThat(typeInfo).isInstanceOf(TupleTypeInfo.class);

        @SuppressWarnings("unchecked")
        TupleTypeInfo<Tuple3<String, Integer, Double>> tupleInfo =
                (TupleTypeInfo<Tuple3<String, Integer, Double>>) typeInfo;

        assertThat(tupleInfo.getArity()).isEqualTo(3);
        assertThat(tupleInfo.getTypeAt(0)).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
        assertThat(tupleInfo.getTypeAt(1)).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
        assertThat(tupleInfo.getTypeAt(2)).isEqualTo(BasicTypeInfo.DOUBLE_TYPE_INFO);
    }

    @Test
    void testQueryableFormatType() {
        InputFormat<?, ?> format = new QueryableInputFormat();
        TypeInformation<?> typeInfo = TypeExtractor.getInputFormatTypes(format);
        assertThat(typeInfo).isEqualTo(BasicTypeInfo.DOUBLE_TYPE_INFO);
    }

    // --------------------------------------------------------------------------------------------
    //  Test formats
    // --------------------------------------------------------------------------------------------

    public static final class DummyFloatInputFormat implements InputFormat<Float, InputSplit> {

        @Override
        public void configure(Configuration parameters) {}

        @Override
        public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
            return null;
        }

        @Override
        public InputSplit[] createInputSplits(int minNumSplits) {
            return null;
        }

        @Override
        public DefaultInputSplitAssigner getInputSplitAssigner(InputSplit[] splits) {
            return null;
        }

        @Override
        public void open(InputSplit split) {}

        @Override
        public boolean reachedEnd() {
            return false;
        }

        @Override
        public Float nextRecord(Float reuse) throws IOException {
            return null;
        }

        @Override
        public void close() {}
    }

    // --------------------------------------------------------------------------------------------

    public static final class DerivedInputFormat extends GenericInputFormat<Short> {

        @Override
        public boolean reachedEnd() {
            return false;
        }

        @Override
        public Short nextRecord(Short reuse) {
            return null;
        }
    }

    // --------------------------------------------------------------------------------------------

    public static final class DerivedTupleInputFormat
            extends GenericInputFormat<Tuple3<String, Short, Double>> {

        @Override
        public boolean reachedEnd() {
            return false;
        }

        @Override
        public Tuple3<String, Short, Double> nextRecord(Tuple3<String, Short, Double> reuse) {
            return null;
        }
    }

    // --------------------------------------------------------------------------------------------

    public static class RelativeInputFormat<T>
            extends GenericInputFormat<Tuple3<String, T, Double>> {

        @Override
        public boolean reachedEnd() {
            return false;
        }

        @Override
        public Tuple3<String, T, Double> nextRecord(Tuple3<String, T, Double> reuse) {
            return null;
        }
    }

    // --------------------------------------------------------------------------------------------

    public static final class FinalRelativeInputFormat extends RelativeInputFormat<Integer> {

        @Override
        public Tuple3<String, Integer, Double> nextRecord(Tuple3<String, Integer, Double> reuse) {
            return null;
        }
    }

    // --------------------------------------------------------------------------------------------

    public static final class QueryableInputFormat
            implements InputFormat<Float, InputSplit>, ResultTypeQueryable<Double> {

        @Override
        public void configure(Configuration parameters) {}

        @Override
        public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
            return null;
        }

        @Override
        public InputSplit[] createInputSplits(int minNumSplits) {
            return null;
        }

        @Override
        public DefaultInputSplitAssigner getInputSplitAssigner(InputSplit[] splits) {
            return null;
        }

        @Override
        public void open(InputSplit split) {}

        @Override
        public boolean reachedEnd() {
            return false;
        }

        @Override
        public Float nextRecord(Float reuse) throws IOException {
            return null;
        }

        @Override
        public void close() {}

        @Override
        public TypeInformation<Double> getProducedType() {
            return BasicTypeInfo.DOUBLE_TYPE_INFO;
        }
    }
}
