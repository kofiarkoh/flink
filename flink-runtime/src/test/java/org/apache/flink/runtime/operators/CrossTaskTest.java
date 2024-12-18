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
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.runtime.operators.testutils.DelayingInfinitiveInputIterator;
import org.apache.flink.runtime.operators.testutils.DriverTestBase;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.operators.testutils.TaskCancelThread;
import org.apache.flink.runtime.operators.testutils.UniformRecordGenerator;
import org.apache.flink.types.Record;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class CrossTaskTest extends DriverTestBase<CrossFunction<Record, Record, Record>> {

    private static final long CROSS_MEM = 1024 * 1024;

    private final double cross_frac;

    private final CountingOutputCollector output = new CountingOutputCollector();

    CrossTaskTest(ExecutionConfig config) {
        super(config, CROSS_MEM, 0);

        cross_frac = (double) CROSS_MEM / this.getMemoryManager().getMemorySize();
    }

    @TestTemplate
    void testBlock1CrossTask() {
        int keyCnt1 = 10;
        int valCnt1 = 1;

        int keyCnt2 = 100;
        int valCnt2 = 4;

        final int expCnt = keyCnt1 * valCnt1 * keyCnt2 * valCnt2;

        setOutput(this.output);

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));

        getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST);
        getTaskConfig().setRelativeMemoryDriver(cross_frac);

        final CrossDriver<Record, Record, Record> testTask = new CrossDriver<>();

        try {
            testDriver(testTask, MockCrossStub.class);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test failed due to an exception.");
        }

        assertThat(this.output.getNumberOfRecords())
                .withFailMessage("Wrong result size.")
                .isEqualTo(expCnt);
    }

    @TestTemplate
    void testBlock2CrossTask() {
        int keyCnt1 = 10;
        int valCnt1 = 1;

        int keyCnt2 = 100;
        int valCnt2 = 4;

        final int expCnt = keyCnt1 * valCnt1 * keyCnt2 * valCnt2;

        setOutput(this.output);

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));

        getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND);
        getTaskConfig().setRelativeMemoryDriver(cross_frac);

        final CrossDriver<Record, Record, Record> testTask = new CrossDriver<>();

        try {
            testDriver(testTask, MockCrossStub.class);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test failed due to an exception.");
        }

        assertThat(this.output.getNumberOfRecords())
                .withFailMessage("Wrong result size.")
                .isEqualTo(expCnt);
    }

    @TestTemplate
    void testFailingBlockCrossTask() {

        int keyCnt1 = 10;
        int valCnt1 = 1;

        int keyCnt2 = 100;
        int valCnt2 = 4;

        setOutput(this.output);

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));

        getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST);
        getTaskConfig().setRelativeMemoryDriver(cross_frac);

        final CrossDriver<Record, Record, Record> testTask = new CrossDriver<>();

        assertThatThrownBy(() -> testDriver(testTask, MockFailingCrossStub.class))
                .isInstanceOf(ExpectedTestException.class);
    }

    @TestTemplate
    void testFailingBlockCrossTask2() {

        int keyCnt1 = 10;
        int valCnt1 = 1;

        int keyCnt2 = 100;
        int valCnt2 = 4;

        setOutput(this.output);

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));

        getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND);
        getTaskConfig().setRelativeMemoryDriver(cross_frac);

        final CrossDriver<Record, Record, Record> testTask = new CrossDriver<>();

        assertThatThrownBy(() -> testDriver(testTask, MockFailingCrossStub.class))
                .isInstanceOf(ExpectedTestException.class);
    }

    @TestTemplate
    void testStream1CrossTask() {
        int keyCnt1 = 10;
        int valCnt1 = 1;

        int keyCnt2 = 100;
        int valCnt2 = 4;

        final int expCnt = keyCnt1 * valCnt1 * keyCnt2 * valCnt2;

        setOutput(this.output);

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));

        getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST);
        getTaskConfig().setRelativeMemoryDriver(cross_frac);

        final CrossDriver<Record, Record, Record> testTask = new CrossDriver<>();

        try {
            testDriver(testTask, MockCrossStub.class);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test failed due to an exception.");
        }

        assertThat(this.output.getNumberOfRecords())
                .withFailMessage("Wrong result size.")
                .isEqualTo(expCnt);
    }

    @TestTemplate
    void testStream2CrossTask() {
        int keyCnt1 = 10;
        int valCnt1 = 1;

        int keyCnt2 = 100;
        int valCnt2 = 4;

        final int expCnt = keyCnt1 * valCnt1 * keyCnt2 * valCnt2;

        setOutput(this.output);

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));

        getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND);
        getTaskConfig().setRelativeMemoryDriver(cross_frac);

        final CrossDriver<Record, Record, Record> testTask = new CrossDriver<>();

        try {
            testDriver(testTask, MockCrossStub.class);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test failed due to an exception.");
        }

        assertThat(this.output.getNumberOfRecords())
                .withFailMessage("Wrong result size.")
                .isEqualTo(expCnt);
    }

    @TestTemplate
    void testFailingStreamCrossTask() {
        int keyCnt1 = 10;
        int valCnt1 = 1;

        int keyCnt2 = 100;
        int valCnt2 = 4;

        setOutput(this.output);

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));

        getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST);
        getTaskConfig().setRelativeMemoryDriver(cross_frac);

        final CrossDriver<Record, Record, Record> testTask = new CrossDriver<>();

        assertThatThrownBy(() -> testDriver(testTask, MockFailingCrossStub.class))
                .isInstanceOf(ExpectedTestException.class);
    }

    @TestTemplate
    void testFailingStreamCrossTask2() {
        int keyCnt1 = 10;
        int valCnt1 = 1;

        int keyCnt2 = 100;
        int valCnt2 = 4;

        setOutput(this.output);

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));

        getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND);
        getTaskConfig().setRelativeMemoryDriver(cross_frac);

        final CrossDriver<Record, Record, Record> testTask = new CrossDriver<>();

        assertThatThrownBy(() -> testDriver(testTask, MockFailingCrossStub.class))
                .isInstanceOf(ExpectedTestException.class);
    }

    @TestTemplate
    void testStreamEmptyInnerCrossTask() {
        int keyCnt1 = 10;
        int valCnt1 = 1;

        int keyCnt2 = 0;
        int valCnt2 = 0;

        final int expCnt = keyCnt1 * valCnt1 * keyCnt2 * valCnt2;

        setOutput(this.output);

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));

        getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST);
        getTaskConfig().setRelativeMemoryDriver(cross_frac);

        final CrossDriver<Record, Record, Record> testTask = new CrossDriver<>();

        try {
            testDriver(testTask, MockCrossStub.class);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test failed due to an exception.");
        }

        assertThat(this.output.getNumberOfRecords())
                .withFailMessage("Wrong result size.")
                .isEqualTo(expCnt);
    }

    @TestTemplate
    void testStreamEmptyOuterCrossTask() {
        int keyCnt1 = 10;
        int valCnt1 = 1;

        int keyCnt2 = 0;
        int valCnt2 = 0;

        final int expCnt = keyCnt1 * valCnt1 * keyCnt2 * valCnt2;

        setOutput(this.output);

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));

        getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND);
        getTaskConfig().setRelativeMemoryDriver(cross_frac);

        final CrossDriver<Record, Record, Record> testTask = new CrossDriver<>();

        try {
            testDriver(testTask, MockCrossStub.class);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test failed due to an exception.");
        }

        assertThat(this.output.getNumberOfRecords())
                .withFailMessage("Wrong result size.")
                .isEqualTo(expCnt);
    }

    @TestTemplate
    void testBlockEmptyInnerCrossTask() {
        int keyCnt1 = 10;
        int valCnt1 = 1;

        int keyCnt2 = 0;
        int valCnt2 = 0;

        final int expCnt = keyCnt1 * valCnt1 * keyCnt2 * valCnt2;

        setOutput(this.output);

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));

        getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST);
        getTaskConfig().setRelativeMemoryDriver(cross_frac);

        final CrossDriver<Record, Record, Record> testTask = new CrossDriver<>();

        try {
            testDriver(testTask, MockCrossStub.class);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test failed due to an exception.");
        }

        assertThat(this.output.getNumberOfRecords())
                .withFailMessage("Wrong result size.")
                .isEqualTo(expCnt);
    }

    @TestTemplate
    void testBlockEmptyOuterCrossTask() {
        int keyCnt1 = 10;
        int valCnt1 = 1;

        int keyCnt2 = 0;
        int valCnt2 = 0;

        final int expCnt = keyCnt1 * valCnt1 * keyCnt2 * valCnt2;

        setOutput(this.output);

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));

        getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND);
        getTaskConfig().setRelativeMemoryDriver(cross_frac);

        final CrossDriver<Record, Record, Record> testTask = new CrossDriver<>();

        try {
            testDriver(testTask, MockCrossStub.class);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test failed due to an exception.");
        }

        assertThat(this.output.getNumberOfRecords())
                .withFailMessage("Wrong result size.")
                .isEqualTo(expCnt);
    }

    @TestTemplate
    void testCancelBlockCrossTaskInit() {
        int keyCnt = 10;
        int valCnt = 1;

        setOutput(this.output);

        addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
        addInput(new DelayingInfinitiveInputIterator(100));

        getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST);
        getTaskConfig().setRelativeMemoryDriver(cross_frac);

        final CrossDriver<Record, Record, Record> testTask = new CrossDriver<>();

        final AtomicBoolean success = new AtomicBoolean(false);

        Thread taskRunner =
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            testDriver(testTask, MockCrossStub.class);
                            success.set(true);
                        } catch (Exception ie) {
                            ie.printStackTrace();
                        }
                    }
                };
        taskRunner.start();

        TaskCancelThread tct = new TaskCancelThread(1, taskRunner, this);
        tct.start();

        try {
            tct.join();
            taskRunner.join();
        } catch (InterruptedException ie) {
            fail("Joining threads failed");
        }

        assertThat(success)
                .withFailMessage("Exception was thrown despite proper canceling.")
                .isTrue();
    }

    @TestTemplate
    void testCancelBlockCrossTaskCrossing() {
        int keyCnt = 10;
        int valCnt = 1;

        setOutput(this.output);

        addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
        addInput(new DelayingInfinitiveInputIterator(100));

        getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND);
        getTaskConfig().setRelativeMemoryDriver(cross_frac);

        final CrossDriver<Record, Record, Record> testTask = new CrossDriver<>();

        final AtomicBoolean success = new AtomicBoolean(false);

        Thread taskRunner =
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            testDriver(testTask, MockCrossStub.class);
                            success.set(true);
                        } catch (Exception ie) {
                            ie.printStackTrace();
                        }
                    }
                };
        taskRunner.start();

        TaskCancelThread tct = new TaskCancelThread(1, taskRunner, this);
        tct.start();

        try {
            tct.join();
            taskRunner.join();
        } catch (InterruptedException ie) {
            fail("Joining threads failed");
        }

        assertThat(success)
                .withFailMessage("Exception was thrown despite proper canceling.")
                .isTrue();
    }

    @TestTemplate
    void testCancelStreamCrossTaskInit() {
        int keyCnt = 10;
        int valCnt = 1;

        setOutput(this.output);

        addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
        addInput(new DelayingInfinitiveInputIterator(100));

        getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST);
        getTaskConfig().setRelativeMemoryDriver(cross_frac);

        final CrossDriver<Record, Record, Record> testTask = new CrossDriver<>();

        final AtomicBoolean success = new AtomicBoolean(false);

        Thread taskRunner =
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            testDriver(testTask, MockCrossStub.class);
                            success.set(true);
                        } catch (Exception ie) {
                            ie.printStackTrace();
                        }
                    }
                };
        taskRunner.start();

        TaskCancelThread tct = new TaskCancelThread(1, taskRunner, this);
        tct.start();

        try {
            tct.join();
            taskRunner.join();
        } catch (InterruptedException ie) {
            fail("Joining threads failed");
        }

        assertThat(success)
                .withFailMessage("Exception was thrown despite proper canceling.")
                .isTrue();
    }

    @TestTemplate
    void testCancelStreamCrossTaskCrossing() {
        int keyCnt = 10;
        int valCnt = 1;

        setOutput(this.output);

        addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
        addInput(new DelayingInfinitiveInputIterator(100));

        getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND);
        getTaskConfig().setRelativeMemoryDriver(cross_frac);

        final CrossDriver<Record, Record, Record> testTask = new CrossDriver<>();

        final AtomicBoolean success = new AtomicBoolean(false);

        Thread taskRunner =
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            testDriver(testTask, MockCrossStub.class);
                            success.set(true);
                        } catch (Exception ie) {
                            ie.printStackTrace();
                        }
                    }
                };
        taskRunner.start();

        TaskCancelThread tct = new TaskCancelThread(1, taskRunner, this);
        tct.start();

        try {
            tct.join();
            taskRunner.join();
        } catch (InterruptedException ie) {
            fail("Joining threads failed");
        }

        assertThat(success)
                .withFailMessage("Exception was thrown despite proper canceling.")
                .isTrue();
    }

    public static final class MockCrossStub implements CrossFunction<Record, Record, Record> {
        private static final long serialVersionUID = 1L;

        @Override
        public Record cross(Record record1, Record record2) throws Exception {
            return record1;
        }
    }

    public static final class MockFailingCrossStub
            implements CrossFunction<Record, Record, Record> {
        private static final long serialVersionUID = 1L;

        private int cnt = 0;

        @Override
        public Record cross(Record record1, Record record2) {
            if (++this.cnt >= 10) {
                throw new ExpectedTestException();
            }
            return record1;
        }
    }
}
