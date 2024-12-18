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

package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.runtime.operators.testutils.DriverTestBase;
import org.apache.flink.runtime.operators.testutils.UniformRecordGenerator;
import org.apache.flink.runtime.testutils.recordutils.RecordComparator;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class CombineTaskExternalITCase extends DriverTestBase<GroupReduceFunction<Record, ?>> {

    private static final long COMBINE_MEM = 3 * 1024 * 1024;

    private final double combine_frac;

    private final ArrayList<Record> outList = new ArrayList<>();

    @SuppressWarnings("unchecked")
    private final RecordComparator comparator =
            new RecordComparator(
                    new int[] {0}, (Class<? extends Value>[]) new Class<?>[] {IntValue.class});

    CombineTaskExternalITCase(ExecutionConfig config) {
        super(config, COMBINE_MEM, 0);

        combine_frac = (double) COMBINE_MEM / this.getMemoryManager().getMemorySize();
    }

    @TestTemplate
    void testSingleLevelMergeCombineTask() {
        final int keyCnt = 40000;
        final int valCnt = 8;

        addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
        addDriverComparator(this.comparator);
        addDriverComparator(this.comparator);
        setOutput(this.outList);

        getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP_COMBINE);
        getTaskConfig().setRelativeMemoryDriver(combine_frac);
        getTaskConfig().setFilehandlesDriver(2);

        final GroupReduceCombineDriver<Record, Record> testTask = new GroupReduceCombineDriver<>();

        try {
            testDriver(testTask, MockCombiningReduceStub.class);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Invoke method caused exception.");
        }

        int expSum = 0;
        for (int i = 1; i < valCnt; i++) {
            expSum += i;
        }

        // wee need to do the final aggregation manually in the test, because the
        // combiner is not guaranteed to do that
        final HashMap<IntValue, IntValue> aggMap = new HashMap<>();
        for (Record record : this.outList) {
            IntValue key = new IntValue();
            IntValue value = new IntValue();
            key = record.getField(0, key);
            value = record.getField(1, value);
            IntValue prevVal = aggMap.get(key);
            if (prevVal != null) {
                aggMap.put(key, new IntValue(prevVal.getValue() + value.getValue()));
            } else {
                aggMap.put(key, value);
            }
        }

        assertThat(aggMap)
                .withFailMessage("Resultset size was %d. Expected was %d", aggMap.size(), keyCnt)
                .hasSize(keyCnt);

        for (IntValue integer : aggMap.values()) {
            assertThat(integer.getValue()).withFailMessage("Incorrect result").isEqualTo(expSum);
        }

        this.outList.clear();
    }

    @TestTemplate
    void testMultiLevelMergeCombineTask() {
        final int keyCnt = 100000;
        final int valCnt = 8;

        addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
        addDriverComparator(this.comparator);
        addDriverComparator(this.comparator);
        setOutput(this.outList);

        getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP_COMBINE);
        getTaskConfig().setRelativeMemoryDriver(combine_frac);
        getTaskConfig().setFilehandlesDriver(2);

        final GroupReduceCombineDriver<Record, Record> testTask = new GroupReduceCombineDriver<>();

        try {
            testDriver(testTask, MockCombiningReduceStub.class);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Invoke method caused exception.");
        }

        int expSum = 0;
        for (int i = 1; i < valCnt; i++) {
            expSum += i;
        }

        // wee need to do the final aggregation manually in the test, because the
        // combiner is not guaranteed to do that
        final HashMap<IntValue, IntValue> aggMap = new HashMap<>();
        for (Record record : this.outList) {
            IntValue key = new IntValue();
            IntValue value = new IntValue();
            key = record.getField(0, key);
            value = record.getField(1, value);
            IntValue prevVal = aggMap.get(key);
            if (prevVal != null) {
                aggMap.put(key, new IntValue(prevVal.getValue() + value.getValue()));
            } else {
                aggMap.put(key, value);
            }
        }

        assertThat(aggMap)
                .withFailMessage("Resultset size was %d. Expected was %d", aggMap.size(), keyCnt)
                .hasSize(keyCnt);

        for (IntValue integer : aggMap.values()) {
            assertThat(integer.getValue()).withFailMessage("Incorrect result").isEqualTo(expSum);
        }

        this.outList.clear();
    }

    // ------------------------------------------------------------------------
    // ------------------------------------------------------------------------

    public static class MockCombiningReduceStub
            implements GroupReduceFunction<Record, Record>, GroupCombineFunction<Record, Record> {
        private static final long serialVersionUID = 1L;

        private final IntValue theInteger = new IntValue();

        @Override
        public void reduce(Iterable<Record> records, Collector<Record> out) {
            Record element = null;
            int sum = 0;

            for (Record next : records) {
                element = next;
                element.getField(1, this.theInteger);

                sum += this.theInteger.getValue();
            }
            this.theInteger.setValue(sum);
            element.setField(1, this.theInteger);
            out.collect(element);
        }

        @Override
        public void combine(Iterable<Record> records, Collector<Record> out) throws Exception {
            reduce(records, out);
        }
    }
}
