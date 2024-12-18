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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.runtime.state.KeyGroupPartitioner;
import org.apache.flink.runtime.state.KeyGroupPartitionerTestBase;
import org.apache.flink.runtime.state.VoidNamespace;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

/** Test of {@link KeyGroupPartitioner} for timers. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class KeyGroupPartitionerForTimersTest
        extends KeyGroupPartitionerTestBase<TimerHeapInternalTimer<Integer, VoidNamespace>> {

    public KeyGroupPartitionerForTimersTest() {
        super(
                (random ->
                        new TimerHeapInternalTimer<>(
                                42L, random.nextInt() & Integer.MAX_VALUE, VoidNamespace.INSTANCE)),
                TimerHeapInternalTimer::getKey);
    }
}
