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

package org.apache.flink.dropwizard.metrics;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for the DropwizardMeterWrapper. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class DropwizardMeterWrapperTest {

    @Test
    void testWrapper() {
        com.codahale.metrics.Meter dropwizardMeter = mock(com.codahale.metrics.Meter.class);
        when(dropwizardMeter.getOneMinuteRate()).thenReturn(1.0);
        when(dropwizardMeter.getCount()).thenReturn(100L);

        DropwizardMeterWrapper wrapper = new DropwizardMeterWrapper(dropwizardMeter);

        assertThat(wrapper.getRate()).isCloseTo(1.0, offset(0.00001));
        assertThat(wrapper.getCount()).isEqualTo(100L);
    }

    @Test
    void testMarkEvent() {
        com.codahale.metrics.Meter dropwizardMeter = mock(com.codahale.metrics.Meter.class);
        DropwizardMeterWrapper wrapper = new DropwizardMeterWrapper(dropwizardMeter);
        wrapper.markEvent();

        verify(dropwizardMeter).mark();
    }

    @Test
    void testMarkEventN() {
        com.codahale.metrics.Meter dropwizardMeter = mock(com.codahale.metrics.Meter.class);
        DropwizardMeterWrapper wrapper = new DropwizardMeterWrapper(dropwizardMeter);
        wrapper.markEvent(10L);

        verify(dropwizardMeter).mark(10L);
    }
}
