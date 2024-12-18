/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// ----------------------------------------------------------------------------
//  This class is largely adapted from "com.google.common.base.Preconditions",
//  which is part of the "Guava" library.
//
//  Because of frequent issues with dependency conflicts, this class was
//  added to the Flink code base to reduce dependency on Guava.
// ----------------------------------------------------------------------------

package org.apache.flink.runtime.metrics;

import org.apache.flink.metrics.View;
import org.apache.flink.util.clock.ManualClock;
import org.apache.flink.util.clock.SystemClock;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TimerGauge}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class TimerGaugeTest {
    private static final long SLEEP = 10;
    private static final long UPDATE_INTERVAL_MILLIS =
            TimeUnit.SECONDS.toMillis(View.UPDATE_INTERVAL_SECONDS);

    @Test
    void testBasicUsage() {
        ManualClock clock = new ManualClock(42_000_000);
        TimerGauge gauge = new TimerGauge(clock, View.UPDATE_INTERVAL_SECONDS);

        gauge.update();
        assertThat(gauge.getValue()).isZero();
        assertThat(gauge.getMaxSingleMeasurement()).isZero();
        assertThat(gauge.getAccumulatedCount()).isZero();

        gauge.markStart();
        clock.advanceTime(SLEEP, TimeUnit.MILLISECONDS);
        gauge.markEnd();
        clock.advanceTime(UPDATE_INTERVAL_MILLIS - SLEEP, TimeUnit.MILLISECONDS);
        gauge.update();

        assertThat(gauge.getValue()).isEqualTo(SLEEP / View.UPDATE_INTERVAL_SECONDS);
        assertThat(gauge.getMaxSingleMeasurement()).isEqualTo(SLEEP);
        assertThat(gauge.getAccumulatedCount()).isEqualTo(SLEEP);

        // Check that the getMaxSingleMeasurement can go down after an update
        gauge.markStart();
        clock.advanceTime(SLEEP / 2, TimeUnit.MILLISECONDS);
        gauge.markEnd();
        clock.advanceTime(UPDATE_INTERVAL_MILLIS - SLEEP / 2, TimeUnit.MILLISECONDS);
        gauge.update();

        assertThat(gauge.getMaxSingleMeasurement()).isEqualTo(SLEEP / 2);
        assertThat(gauge.getAccumulatedCount()).isEqualTo(SLEEP + SLEEP / 2);
    }

    @Test
    void testUpdateWithoutMarkingEnd() {
        ManualClock clock = new ManualClock(42_000_000);
        TimerGauge gauge = new TimerGauge(clock, 2 * View.UPDATE_INTERVAL_SECONDS);

        clock.advanceTime(UPDATE_INTERVAL_MILLIS - SLEEP, TimeUnit.MILLISECONDS);
        gauge.markStart();
        clock.advanceTime(SLEEP, TimeUnit.MILLISECONDS);
        gauge.update();

        assertThat(gauge.getValue()).isEqualTo(SLEEP / View.UPDATE_INTERVAL_SECONDS);
        assertThat(gauge.getMaxSingleMeasurement()).isEqualTo(SLEEP);

        // keep the measurement going for another update
        clock.advanceTime(UPDATE_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        gauge.update();

        assertThat(gauge.getValue())
                .isEqualTo((SLEEP + UPDATE_INTERVAL_MILLIS) / (2 * View.UPDATE_INTERVAL_SECONDS));
        // max single measurement is now spanning two updates
        assertThat(gauge.getMaxSingleMeasurement()).isEqualTo(SLEEP + UPDATE_INTERVAL_MILLIS);
    }

    @Test
    void testGetWithoutUpdate() {
        ManualClock clock = new ManualClock(42_000_000);
        TimerGauge gauge = new TimerGauge(clock);

        gauge.markStart();
        clock.advanceTime(SLEEP, TimeUnit.MILLISECONDS);

        assertThat(gauge.getValue()).isZero();

        gauge.markEnd();

        assertThat(gauge.getValue()).isZero();
        assertThat(gauge.getMaxSingleMeasurement()).isZero();
    }

    @Test
    void testUpdateBeforeMarkingEnd() {
        ManualClock clock = new ManualClock(42_000_000);
        // this timer gauge measures 2 update intervals
        TimerGauge gauge = new TimerGauge(clock, 2 * View.UPDATE_INTERVAL_SECONDS);

        // interval 1
        clock.advanceTime(UPDATE_INTERVAL_MILLIS - SLEEP, TimeUnit.MILLISECONDS);
        gauge.markStart();
        clock.advanceTime(SLEEP, TimeUnit.MILLISECONDS);
        gauge.update();
        // interval 2
        clock.advanceTime(SLEEP, TimeUnit.MILLISECONDS);
        gauge.markEnd();
        clock.advanceTime(UPDATE_INTERVAL_MILLIS - SLEEP, TimeUnit.MILLISECONDS);
        gauge.update();

        assertThat(gauge.getValue()).isEqualTo(SLEEP / View.UPDATE_INTERVAL_SECONDS);
        assertThat(gauge.getMaxSingleMeasurement()).isEqualTo(2 * SLEEP);
    }

    @Test
    void testLargerTimespan() {
        ManualClock clock = new ManualClock(42_000_000);
        TimerGauge gauge = new TimerGauge(clock, 2 * View.UPDATE_INTERVAL_SECONDS);

        gauge.markStart();
        clock.advanceTime(SLEEP, TimeUnit.MILLISECONDS);
        gauge.markEnd();
        clock.advanceTime(UPDATE_INTERVAL_MILLIS - SLEEP, TimeUnit.MILLISECONDS);
        gauge.update();

        assertThat(gauge.getValue()).isEqualTo(SLEEP / View.UPDATE_INTERVAL_SECONDS);
        assertThat(gauge.getMaxSingleMeasurement()).isEqualTo(SLEEP);
        assertThat(gauge.getAccumulatedCount()).isEqualTo(SLEEP);

        clock.advanceTime(UPDATE_INTERVAL_MILLIS - SLEEP, TimeUnit.MILLISECONDS);
        gauge.update();
        // One sleep in 2 intervals
        assertThat(gauge.getValue()).isEqualTo(SLEEP / (View.UPDATE_INTERVAL_SECONDS * 2));
        assertThat(gauge.getMaxSingleMeasurement()).isZero();
        assertThat(gauge.getAccumulatedCount()).isEqualTo(SLEEP);

        // One sleep in each interval

        gauge.markStart();
        clock.advanceTime(SLEEP, TimeUnit.MILLISECONDS);
        gauge.markEnd();
        clock.advanceTime(UPDATE_INTERVAL_MILLIS - SLEEP, TimeUnit.MILLISECONDS);
        gauge.update();

        gauge.markStart();
        clock.advanceTime(SLEEP, TimeUnit.MILLISECONDS);
        gauge.markEnd();
        clock.advanceTime(UPDATE_INTERVAL_MILLIS - SLEEP, TimeUnit.MILLISECONDS);
        gauge.update();

        assertThat(gauge.getValue()).isEqualTo(SLEEP / (View.UPDATE_INTERVAL_SECONDS));

        // Check that the getMaxSingleMeasurement can go down after an update
        gauge.markStart();
        clock.advanceTime(SLEEP / 2, TimeUnit.MILLISECONDS);
        gauge.markEnd();
        clock.advanceTime(UPDATE_INTERVAL_MILLIS - SLEEP / 2, TimeUnit.MILLISECONDS);
        gauge.update();

        assertThat(gauge.getMaxSingleMeasurement()).isEqualTo(SLEEP / 2);
        assertThat(gauge.getAccumulatedCount()).isEqualTo(3 * SLEEP + SLEEP / 2);
    }

    @Test
    void testListeners() {
        TimerGauge gauge = new TimerGauge(SystemClock.getInstance(), View.UPDATE_INTERVAL_SECONDS);
        TestStartStopListener listener1 = new TestStartStopListener();
        TestStartStopListener listener2 = new TestStartStopListener();

        gauge.registerListener(listener1);

        gauge.markStart();
        listener1.assertCounts(1, 0);
        gauge.markEnd();
        listener1.assertCounts(1, 1);

        gauge.markStart();
        gauge.registerListener(listener2);
        listener1.assertCounts(2, 1);
        listener2.assertCounts(1, 0);
        gauge.markEnd();
        listener1.assertCounts(2, 2);
        listener2.assertCounts(1, 1);

        gauge.markStart();
        gauge.unregisterListener(listener1);
        listener1.assertCounts(3, 3);
        listener2.assertCounts(2, 1);

        gauge.markEnd();
        listener2.assertCounts(2, 2);

        gauge.unregisterListener(listener2);

        gauge.markStart();
        gauge.markEnd();
        listener1.assertCounts(3, 3);
        listener2.assertCounts(2, 2);
    }

    static class TestStartStopListener implements TimerGauge.StartStopListener {
        long startCount;
        long endCount;

        @Override
        public void markStart() {
            startCount++;
        }

        @Override
        public void markEnd() {
            endCount++;
        }

        public void assertCounts(long expectedStart, long expectedEnd) {
            assertThat(startCount).isEqualTo(expectedStart);
            assertThat(endCount).isEqualTo(expectedEnd);
        }
    }
}
