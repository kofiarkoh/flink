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

package org.apache.flink.cep.nfa;

import org.apache.flink.cep.Event;
import org.apache.flink.cep.SubEvent;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.utils.NFATestHarness;
import org.apache.flink.cep.utils.TestSharedBuffer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Tests that check if we do not degrade NFA computation in case of State accesses. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class NFAStateAccessTest {

    @Test
    public void testComplexBranchingAfterZeroOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event middleEvent3 = new Event(43, "a", 4.0);
        Event end1 = new Event(44, "b", 5.0);
        Event end2 = new Event(45, "d", 6.0);
        Event end3 = new Event(46, "d", 7.0);
        Event end4 = new Event(47, "e", 8.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(middleEvent3, 5));
        inputEvents.add(new StreamRecord<>(end1, 6));
        inputEvents.add(new StreamRecord<>(end2, 7));
        inputEvents.add(new StreamRecord<>(end3, 8));
        inputEvents.add(new StreamRecord<>(end4, 9));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .allowCombinations()
                        .optional()
                        .followedByAny("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .followedByAny("end2")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")))
                        .followedByAny("end3")
                        .where(SimpleCondition.of(value -> value.getName().equals("e")));

        TestSharedBuffer<Event> sharedBuffer =
                TestSharedBuffer.createTestBuffer(Event.createTypeSerializer());
        NFATestHarness nfaTestHarness =
                NFATestHarness.forPattern(pattern).withSharedBuffer(sharedBuffer).build();
        nfaTestHarness.consumeRecords(inputEvents);

        assertEquals(58, sharedBuffer.getStateReads());
        assertEquals(41, sharedBuffer.getStateWrites());
        assertEquals(99, sharedBuffer.getStateAccesses());
    }

    @Test
    public void testIterativeWithABACPattern() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        final Event startEvent1 = new Event(40, "start", 1.0);
        final Event startEvent2 = new Event(40, "start", 2.0);
        final Event startEvent3 = new Event(40, "start", 3.0);
        final Event startEvent4 = new Event(40, "start", 4.0);
        final SubEvent middleEvent1 = new SubEvent(41, "foo1", 1.0, 10);
        final SubEvent middleEvent2 = new SubEvent(42, "foo2", 2.0, 10);
        final SubEvent middleEvent3 = new SubEvent(43, "foo3", 3.0, 10);
        final SubEvent middleEvent4 = new SubEvent(43, "foo4", 1.0, 10);
        final Event endEvent = new Event(46, "end", 1.0);

        inputEvents.add(new StreamRecord<>(startEvent1, 1L)); // 1
        inputEvents.add(new StreamRecord<Event>(middleEvent1, 2L)); // 1

        inputEvents.add(new StreamRecord<>(startEvent2, 2L)); // 2
        inputEvents.add(new StreamRecord<>(startEvent3, 2L)); // 3
        inputEvents.add(new StreamRecord<Event>(middleEvent2, 2L)); // 2

        inputEvents.add(new StreamRecord<>(startEvent4, 2L)); // 4
        inputEvents.add(new StreamRecord<Event>(middleEvent3, 2L)); // 3
        inputEvents.add(new StreamRecord<Event>(middleEvent4, 2L)); // 1
        inputEvents.add(new StreamRecord<>(endEvent, 4L));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .followedByAny("middle1")
                        .subtype(SubEvent.class)
                        .where(SimpleCondition.of(value -> value.getName().startsWith("foo")))
                        .followedBy("middle2")
                        .where(
                                new IterativeCondition<Event>() {
                                    private static final long serialVersionUID =
                                            -1223388426808292695L;

                                    @Override
                                    public boolean filter(Event value, Context<Event> ctx)
                                            throws Exception {
                                        if (!value.getName().equals("start")) {
                                            return false;
                                        }

                                        double sum = 0.0;
                                        for (Event e : ctx.getEventsForPattern("middle2")) {
                                            sum += e.getPrice();
                                        }
                                        sum += value.getPrice();
                                        return Double.compare(sum, 5.0) <= 0;
                                    }
                                })
                        .oneOrMore()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("end")));

        TestSharedBuffer<Event> sharedBuffer =
                TestSharedBuffer.createTestBuffer(Event.createTypeSerializer());
        NFATestHarness nfaTestHarness =
                NFATestHarness.forPattern(pattern).withSharedBuffer(sharedBuffer).build();
        nfaTestHarness.consumeRecords(inputEvents);

        assertEquals(90, sharedBuffer.getStateReads());
        assertEquals(34, sharedBuffer.getStateWrites());
        assertEquals(124, sharedBuffer.getStateAccesses());
    }
}
