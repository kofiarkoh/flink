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
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.WithinType;
import org.apache.flink.cep.pattern.conditions.BooleanConditions;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.utils.NFATestHarness;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.TestLogger;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cep.utils.NFAUtils.compile;
import static org.junit.Assert.assertEquals;

/** Tests for {@link NFA}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class NFATest extends TestLogger {
    @Test
    public void testSimpleNFA() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        streamEvents.add(new StreamRecord<>(new Event(1, "start", 1.0), 1L));
        streamEvents.add(new StreamRecord<>(new Event(2, "bar", 2.0), 2L));
        streamEvents.add(new StreamRecord<>(new Event(3, "start", 3.0), 3L));
        streamEvents.add(new StreamRecord<>(new Event(4, "end", 4.0), 4L));

        State<Event> startState = new State<>("start", State.StateType.Start);
        State<Event> endState = new State<>("end", State.StateType.Normal);
        State<Event> endingState = new State<>("", State.StateType.Final);

        startState.addTake(endState, SimpleCondition.of(value -> value.getName().equals("start")));
        endState.addTake(endingState, SimpleCondition.of(value -> value.getName().equals("end")));
        endState.addIgnore(BooleanConditions.<Event>trueFunction());

        List<State<Event>> states = new ArrayList<>();
        states.add(startState);
        states.add(endState);
        states.add(endingState);

        List<Map<String, List<Event>>> expectedPatterns = new ArrayList<>();

        Map<String, List<Event>> firstPattern = new HashMap<>();
        firstPattern.put("start", Collections.singletonList(new Event(1, "start", 1.0)));
        firstPattern.put("end", Collections.singletonList(new Event(4, "end", 4.0)));

        Map<String, List<Event>> secondPattern = new HashMap<>();
        secondPattern.put("start", Collections.singletonList(new Event(3, "start", 3.0)));
        secondPattern.put("end", Collections.singletonList(new Event(4, "end", 4.0)));

        expectedPatterns.add(firstPattern);
        expectedPatterns.add(secondPattern);

        NFA<Event> nfa = new NFA<>(states, Collections.emptyMap(), 0, false);
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).build();

        Collection<Map<String, List<Event>>> actualPatterns =
                nfaTestHarness.consumeRecords(streamEvents);

        assertEquals(expectedPatterns, actualPatterns);
    }

    @Test
    public void testTimeoutWindowPruningWithinFirstAndLast() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        streamEvents.add(new StreamRecord<>(new Event(1, "start", 1.0), 1L));
        streamEvents.add(new StreamRecord<>(new Event(2, "bar", 2.0), 2L));
        streamEvents.add(new StreamRecord<>(new Event(3, "start", 3.0), 3L));
        streamEvents.add(new StreamRecord<>(new Event(4, "end", 4.0), 4L));

        List<Map<String, List<Event>>> expectedPatterns = new ArrayList<>();

        Map<String, List<Event>> secondPattern = new HashMap<>();
        secondPattern.put("start", Collections.singletonList(new Event(3, "start", 3.0)));
        secondPattern.put("end", Collections.singletonList(new Event(4, "end", 4.0)));

        expectedPatterns.add(secondPattern);

        NFA<Event> nfa = createStartEndNFA();
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).build();

        Collection<Map<String, List<Event>>> actualPatterns =
                nfaTestHarness.consumeRecords(streamEvents);

        assertEquals(expectedPatterns, actualPatterns);
    }

    @Test
    public void testTimeoutWindowPruningWithinPreviousAndNext() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        streamEvents.add(new StreamRecord<>(new Event(1, "start", 1.0), 1L));
        streamEvents.add(new StreamRecord<>(new Event(2, "end", 2.0), 2L));
        streamEvents.add(new StreamRecord<>(new Event(3, "start", 3.0), 3L));
        streamEvents.add(new StreamRecord<>(new Event(4, "end", 4.0), 6L));
        streamEvents.add(new StreamRecord<>(new Event(5, "start", 5.0), 7L));
        streamEvents.add(new StreamRecord<>(new Event(6, "end", 6.0), 8L));

        List<Map<String, List<Event>>> expectedPatterns = new ArrayList<>();

        Map<String, List<Event>> secondPattern = new HashMap<>();
        secondPattern.put("start", Collections.singletonList(new Event(1, "start", 1.0)));
        secondPattern.put("end", Collections.singletonList(new Event(2, "end", 2.0)));

        expectedPatterns.add(secondPattern);

        secondPattern = new HashMap<>();
        secondPattern.put("start", Collections.singletonList(new Event(5, "start", 5.0)));
        secondPattern.put("end", Collections.singletonList(new Event(6, "end", 6.0)));

        expectedPatterns.add(secondPattern);

        NFA<Event> nfa = createStartEndNFA(WithinType.PREVIOUS_AND_CURRENT);
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).build();

        Collection<Map<String, List<Event>>> actualPatterns =
                nfaTestHarness.consumeRecords(streamEvents);

        assertEquals(expectedPatterns, actualPatterns);
    }

    /**
     * Tests that elements whose timestamp difference is exactly the window length are not matched.
     * The reason is that the right window side (later elements) is exclusive.
     */
    @Test
    public void testWindowBorders() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        streamEvents.add(new StreamRecord<>(new Event(1, "start", 1.0), 1L));
        streamEvents.add(new StreamRecord<>(new Event(2, "end", 2.0), 3L));

        List<Map<String, List<Event>>> expectedPatterns = Collections.emptyList();

        NFA<Event> nfa = createStartEndNFA();
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).build();

        Collection<Map<String, List<Event>>> actualPatterns =
                nfaTestHarness.consumeRecords(streamEvents);

        assertEquals(expectedPatterns, actualPatterns);
    }

    /**
     * Tests that pruning shared buffer elements and computations state use the same window border
     * semantics (left side inclusive and right side exclusive).
     */
    @Test
    public void testTimeoutWindowPruningWindowBorders() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        streamEvents.add(new StreamRecord<>(new Event(1, "start", 1.0), 1L));
        streamEvents.add(new StreamRecord<>(new Event(2, "start", 2.0), 2L));
        streamEvents.add(new StreamRecord<>(new Event(3, "foobar", 3.0), 3L));
        streamEvents.add(new StreamRecord<>(new Event(4, "end", 4.0), 3L));

        List<Map<String, List<Event>>> expectedPatterns = new ArrayList<>();

        Map<String, List<Event>> secondPattern = new HashMap<>();
        secondPattern.put("start", Collections.singletonList(new Event(2, "start", 2.0)));
        secondPattern.put("end", Collections.singletonList(new Event(4, "end", 4.0)));

        expectedPatterns.add(secondPattern);

        NFA<Event> nfa = createStartEndNFA();
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).build();

        Collection<Map<String, List<Event>>> actualPatterns =
                nfaTestHarness.consumeRecords(streamEvents);

        assertEquals(expectedPatterns, actualPatterns);
    }

    @Test
    public void testNFASerialization() throws Exception {
        Pattern<Event, ?> pattern1 =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .oneOrMore()
                        .optional()
                        .allowCombinations()
                        .followedByAny("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        Pattern<Event, ?> pattern2 =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .notFollowedBy("not")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .oneOrMore()
                        .optional()
                        .allowCombinations()
                        .followedByAny("end")
                        .where(
                                new IterativeCondition<Event>() {
                                    private static final long serialVersionUID =
                                            8061969839441121955L;

                                    @Override
                                    public boolean filter(
                                            Event value, IterativeCondition.Context<Event> ctx)
                                            throws Exception {
                                        double sum = 0.0;
                                        for (Event e : ctx.getEventsForPattern("middle")) {
                                            sum += e.getPrice();
                                        }
                                        return sum > 5.0;
                                    }
                                });

        Pattern<Event, ?> pattern3 =
                Pattern.<Event>begin("start")
                        .notFollowedBy("not")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .oneOrMore()
                        .allowCombinations()
                        .followedByAny("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        List<Pattern<Event, ?>> patterns = new ArrayList<>();
        patterns.add(pattern1);
        patterns.add(pattern2);
        patterns.add(pattern3);

        for (Pattern<Event, ?> p : patterns) {
            NFA<Event> nfa = compile(p, false);

            Event a = new Event(40, "a", 1.0);
            Event b = new Event(41, "b", 2.0);
            Event c = new Event(42, "c", 3.0);
            Event b1 = new Event(41, "b", 3.0);
            Event b2 = new Event(41, "b", 4.0);
            Event b3 = new Event(41, "b", 5.0);
            Event d = new Event(43, "d", 4.0);

            NFAState nfaState = nfa.createInitialNFAState();

            NFATestHarness nfaTestHarness =
                    NFATestHarness.forNFA(nfa).withNFAState(nfaState).build();

            nfaTestHarness.consumeRecord(new StreamRecord<>(a, 1));
            nfaTestHarness.consumeRecord(new StreamRecord<>(b, 2));
            nfaTestHarness.consumeRecord(new StreamRecord<>(c, 3));
            nfaTestHarness.consumeRecord(new StreamRecord<>(b1, 4));
            nfaTestHarness.consumeRecord(new StreamRecord<>(b2, 5));
            nfaTestHarness.consumeRecord(new StreamRecord<>(b3, 6));
            nfaTestHarness.consumeRecord(new StreamRecord<>(d, 7));
            nfaTestHarness.consumeRecord(new StreamRecord<>(a, 8));

            NFAStateSerializer serializer = new NFAStateSerializer();

            // serialize
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            serializer.serialize(nfaState, new DataOutputViewStreamWrapper(baos));
            baos.close();

            // copy
            ByteArrayInputStream in = new ByteArrayInputStream(baos.toByteArray());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            serializer
                    .duplicate()
                    .copy(new DataInputViewStreamWrapper(in), new DataOutputViewStreamWrapper(out));
            in.close();
            out.close();

            // deserialize
            ByteArrayInputStream bais = new ByteArrayInputStream(out.toByteArray());
            NFAState copy =
                    serializer.duplicate().deserialize(new DataInputViewStreamWrapper(bais));
            bais.close();
            assertEquals(nfaState, copy);
        }
    }

    private NFA<Event> createStartEndNFA() {
        return createStartEndNFA(WithinType.FIRST_AND_LAST);
    }

    private NFA<Event> createStartEndNFA(WithinType withinType) {
        State<Event> startState = new State<>("start", State.StateType.Start);
        State<Event> endState = new State<>("end", State.StateType.Normal);
        State<Event> endingState = new State<>("", State.StateType.Final);

        startState.addTake(endState, SimpleCondition.of(value -> value.getName().equals("start")));
        endState.addTake(endingState, SimpleCondition.of(value -> value.getName().equals("end")));
        endState.addIgnore(BooleanConditions.<Event>trueFunction());

        List<State<Event>> states = new ArrayList<>();
        states.add(startState);
        states.add(endState);
        states.add(endingState);

        boolean withinFirstAndLast = WithinType.FIRST_AND_LAST.equals(withinType);
        return new NFA<>(
                states,
                withinFirstAndLast ? Collections.emptyMap() : Collections.singletonMap("end", 2L),
                withinFirstAndLast ? 2L : 0L,
                false);
    }
}
