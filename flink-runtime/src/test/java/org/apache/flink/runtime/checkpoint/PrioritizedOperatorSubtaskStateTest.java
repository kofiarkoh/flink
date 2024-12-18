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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.Preconditions;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewInputChannelStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewKeyedStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewOperatorStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewResultSubpartitionStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.deepDummyCopy;
import static org.apache.flink.runtime.checkpoint.StateObjectCollection.singleton;
import static org.assertj.core.api.Assertions.assertThat;

/** {@link PrioritizedOperatorSubtaskState} test. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class PrioritizedOperatorSubtaskStateTest {

    private static final Random RANDOM = new Random(0x42);

    @Test
    void testTryCreateMixedLocalAndRemoteAlternative() {
        testTryCreateMixedLocalAndRemoteAlternative(
                StateHandleDummyUtil::createKeyedStateHandleFromSeed,
                KeyedStateHandle::getKeyGroupRange);
    }

    <SH extends StateObject, ID> void testTryCreateMixedLocalAndRemoteAlternative(
            IntFunction<SH> stateHandleFactory, Function<SH, ID> idExtractor) {

        SH remote0 = stateHandleFactory.apply(0);
        SH remote1 = stateHandleFactory.apply(1);
        SH remote2 = stateHandleFactory.apply(2);
        SH remote3 = stateHandleFactory.apply(3);

        List<SH> jmState = Arrays.asList(remote0, remote1, remote2, remote3);

        SH local0 = stateHandleFactory.apply(0);
        SH local3a = stateHandleFactory.apply(3);

        List<SH> alternativeA = Arrays.asList(local0, local3a);

        SH local1 = stateHandleFactory.apply(1);
        SH local3b = stateHandleFactory.apply(3);
        SH local5 = stateHandleFactory.apply(5);

        List<SH> alternativeB = Arrays.asList(local1, local3b, local5);

        List<StateObjectCollection<SH>> alternatives =
                Arrays.asList(
                        new StateObjectCollection<>(alternativeA),
                        new StateObjectCollection<>(Collections.emptyList()),
                        new StateObjectCollection<>(alternativeB));

        StateObjectCollection<SH> result =
                PrioritizedOperatorSubtaskState.Builder.tryComputeMixedLocalAndRemoteAlternative(
                                new StateObjectCollection<>(jmState), alternatives, idExtractor)
                        .get();

        assertThat(result).hasSameElementsAs(Arrays.asList(local0, local1, remote2, local3a));
    }

    @Test
    void testTryCreateMixedLocalAndRemoteAlternativeEmptyAlternative() {
        testTryCreateMixedLocalAndRemoteAlternativeEmptyAlternative(
                StateHandleDummyUtil::createKeyedStateHandleFromSeed,
                KeyedStateHandle::getKeyGroupRange);
    }

    <SH extends StateObject, ID> void testTryCreateMixedLocalAndRemoteAlternativeEmptyAlternative(
            IntFunction<SH> stateHandleFactory, Function<SH, ID> idExtractor) {
        List<SH> jmState =
                Arrays.asList(
                        stateHandleFactory.apply(0),
                        stateHandleFactory.apply(1),
                        stateHandleFactory.apply(2),
                        stateHandleFactory.apply(3));

        Assertions.assertFalse(
                PrioritizedOperatorSubtaskState.Builder.tryComputeMixedLocalAndRemoteAlternative(
                                new StateObjectCollection<>(jmState),
                                Collections.emptyList(),
                                idExtractor)
                        .isPresent());

        Assertions.assertFalse(
                PrioritizedOperatorSubtaskState.Builder.tryComputeMixedLocalAndRemoteAlternative(
                                new StateObjectCollection<>(jmState),
                                Collections.singletonList(new StateObjectCollection<>()),
                                idExtractor)
                        .isPresent());
    }

    @Test
    void testTryCreateMixedLocalAndRemoteAlternativeEmptyJMState() {
        testTryCreateMixedLocalAndRemoteAlternativeEmptyJMState(
                StateHandleDummyUtil::createKeyedStateHandleFromSeed,
                KeyedStateHandle::getKeyGroupRange);
    }

    <SH extends StateObject, ID> void testTryCreateMixedLocalAndRemoteAlternativeEmptyJMState(
            IntFunction<SH> stateHandleFactory, Function<SH, ID> idExtractor) {
        List<SH> alternativeA =
                Arrays.asList(stateHandleFactory.apply(0), stateHandleFactory.apply(3));

        Assertions.assertFalse(
                PrioritizedOperatorSubtaskState.Builder.tryComputeMixedLocalAndRemoteAlternative(
                                new StateObjectCollection<>(Collections.emptyList()),
                                Collections.singletonList(
                                        new StateObjectCollection<>(alternativeA)),
                                idExtractor)
                        .isPresent());

        Assertions.assertFalse(
                PrioritizedOperatorSubtaskState.Builder.tryComputeMixedLocalAndRemoteAlternative(
                                new StateObjectCollection<>(Collections.emptyList()),
                                Collections.emptyList(),
                                KeyedStateHandle::getKeyGroupRange)
                        .isPresent());
    }

    /**
     * This tests attempts to test (almost) the full space of significantly different options for
     * verifying and prioritizing {@link OperatorSubtaskState} options for local recovery over
     * primary/remote state handles.
     */
    @Test
    void testPrioritization() {

        for (int i = 0; i < 81; ++i) { // 3^4 possible configurations.

            OperatorSubtaskState primaryAndFallback = generateForConfiguration(i);

            for (int j = 0; j < 9; ++j) { // we test 3^2 configurations.
                CreateAltSubtaskStateMode modeFirst = CreateAltSubtaskStateMode.byCode(j % 3);
                OperatorSubtaskState bestAlternative =
                        modeFirst.createAlternativeSubtaskState(primaryAndFallback);
                CreateAltSubtaskStateMode modeSecond =
                        CreateAltSubtaskStateMode.byCode((j / 3) % 3);
                OperatorSubtaskState secondBestAlternative =
                        modeSecond.createAlternativeSubtaskState(primaryAndFallback);

                List<OperatorSubtaskState> orderedAlternativesList =
                        Arrays.asList(bestAlternative, secondBestAlternative);
                List<OperatorSubtaskState> validAlternativesList = new ArrayList<>(3);
                if (modeFirst == CreateAltSubtaskStateMode.ONE_VALID_STATE_HANDLE) {
                    validAlternativesList.add(bestAlternative);
                }
                if (modeSecond == CreateAltSubtaskStateMode.ONE_VALID_STATE_HANDLE) {
                    validAlternativesList.add(secondBestAlternative);
                }
                validAlternativesList.add(primaryAndFallback);

                PrioritizedOperatorSubtaskState.Builder builder =
                        new PrioritizedOperatorSubtaskState.Builder(
                                primaryAndFallback, orderedAlternativesList);

                PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskState = builder.build();

                OperatorSubtaskState[] validAlternatives =
                        validAlternativesList.toArray(new OperatorSubtaskState[0]);

                OperatorSubtaskState[] onlyPrimary =
                        new OperatorSubtaskState[] {primaryAndFallback};

                assertThat(
                                checkResultAsExpected(
                                        OperatorSubtaskState::getManagedOperatorState,
                                        PrioritizedOperatorSubtaskState
                                                ::getPrioritizedManagedOperatorState,
                                        prioritizedOperatorSubtaskState,
                                        primaryAndFallback.getManagedOperatorState().size() == 1
                                                ? validAlternatives
                                                : onlyPrimary))
                        .isTrue();

                StateObjectCollection<KeyedStateHandle> expManagedKeyed =
                        computeExpectedMixedState(
                                orderedAlternativesList,
                                primaryAndFallback,
                                OperatorSubtaskState::getManagedKeyedState,
                                KeyedStateHandle::getKeyGroupRange);

                assertResultAsExpected(
                        expManagedKeyed,
                        primaryAndFallback.getManagedKeyedState(),
                        prioritizedOperatorSubtaskState.getPrioritizedManagedKeyedState());

                assertThat(
                                checkResultAsExpected(
                                        OperatorSubtaskState::getRawOperatorState,
                                        PrioritizedOperatorSubtaskState
                                                ::getPrioritizedRawOperatorState,
                                        prioritizedOperatorSubtaskState,
                                        primaryAndFallback.getRawOperatorState().size() == 1
                                                ? validAlternatives
                                                : onlyPrimary))
                        .isTrue();

                StateObjectCollection<KeyedStateHandle> expRawKeyed =
                        computeExpectedMixedState(
                                orderedAlternativesList,
                                primaryAndFallback,
                                OperatorSubtaskState::getRawKeyedState,
                                KeyedStateHandle::getKeyGroupRange);

                assertResultAsExpected(
                        expRawKeyed,
                        primaryAndFallback.getRawKeyedState(),
                        prioritizedOperatorSubtaskState.getPrioritizedRawKeyedState());
            }
        }
    }

    /**
     * Generator for all 3^4 = 81 possible configurations of a OperatorSubtaskState: - 4 different
     * sub-states: managed/raw + operator/keyed. - 3 different options per sub-state: empty
     * (simulate no state), single handle (simulate recovery), 2 handles (simulate e.g. rescaling)
     */
    private OperatorSubtaskState generateForConfiguration(int conf) {

        Preconditions.checkState(conf >= 0 && conf <= 80); // 3^4
        final int numModes = 3;

        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 4);
        KeyGroupRange keyGroupRange1 = new KeyGroupRange(0, 2);
        KeyGroupRange keyGroupRange2 = new KeyGroupRange(3, 4);

        int div = 1;
        int mode = (conf / div) % numModes;
        StateObjectCollection<OperatorStateHandle> s1 =
                mode == 0
                        ? StateObjectCollection.empty()
                        : mode == 1
                                ? new StateObjectCollection<>(
                                        Collections.singletonList(
                                                createNewOperatorStateHandle(2, RANDOM)))
                                : new StateObjectCollection<>(
                                        Arrays.asList(
                                                createNewOperatorStateHandle(2, RANDOM),
                                                createNewOperatorStateHandle(2, RANDOM)));
        div *= numModes;
        mode = (conf / div) % numModes;
        StateObjectCollection<OperatorStateHandle> s2 =
                mode == 0
                        ? StateObjectCollection.empty()
                        : mode == 1
                                ? new StateObjectCollection<>(
                                        Collections.singletonList(
                                                createNewOperatorStateHandle(2, RANDOM)))
                                : new StateObjectCollection<>(
                                        Arrays.asList(
                                                createNewOperatorStateHandle(2, RANDOM),
                                                createNewOperatorStateHandle(2, RANDOM)));

        div *= numModes;
        mode = (conf / div) % numModes;
        StateObjectCollection<KeyedStateHandle> s3 =
                mode == 0
                        ? StateObjectCollection.empty()
                        : mode == 1
                                ? new StateObjectCollection<>(
                                        Collections.singletonList(
                                                createNewKeyedStateHandle(keyGroupRange)))
                                : new StateObjectCollection<>(
                                        Arrays.asList(
                                                createNewKeyedStateHandle(keyGroupRange1),
                                                createNewKeyedStateHandle(keyGroupRange2)));

        div *= numModes;
        mode = (conf / div) % numModes;
        StateObjectCollection<KeyedStateHandle> s4 =
                mode == 0
                        ? StateObjectCollection.empty()
                        : mode == 1
                                ? new StateObjectCollection<>(
                                        Collections.singletonList(
                                                createNewKeyedStateHandle(keyGroupRange)))
                                : new StateObjectCollection<>(
                                        Arrays.asList(
                                                createNewKeyedStateHandle(keyGroupRange1),
                                                createNewKeyedStateHandle(keyGroupRange2)));

        return OperatorSubtaskState.builder()
                .setManagedOperatorState(s1)
                .setRawOperatorState(s2)
                .setManagedKeyedState(s3)
                .setRawKeyedState(s4)
                .build();
    }

    private enum CreateAltSubtaskStateMode {
        /** mode 0: one valid state handle (deep copy of original). */
        ONE_VALID_STATE_HANDLE(0) {
            @Override
            public OperatorSubtaskState createAlternativeSubtaskState(
                    OperatorSubtaskState primaryOriginal) {
                return OperatorSubtaskState.builder()
                        .setManagedOperatorState(
                                deepCopyFirstElement(primaryOriginal.getManagedOperatorState()))
                        .setRawOperatorState(
                                deepCopyFirstElement(primaryOriginal.getRawOperatorState()))
                        .setManagedKeyedState(
                                deepCopyFirstElement(primaryOriginal.getManagedKeyedState()))
                        .setRawKeyedState(deepCopyFirstElement(primaryOriginal.getRawKeyedState()))
                        .setInputChannelState(deepCopy(primaryOriginal.getInputChannelState()))
                        .setResultSubpartitionState(
                                deepCopy(primaryOriginal.getResultSubpartitionState()))
                        .build();
            }
        },
        /** mode 1: empty StateHandleCollection. */
        EMPTY_STATE_HANDLE_COLLECTION(1) {
            @Override
            public OperatorSubtaskState createAlternativeSubtaskState(
                    OperatorSubtaskState primaryOriginal) {
                return OperatorSubtaskState.builder().build();
            }
        },
        /**
         * mode 2: one invalid state handle (e.g. wrong key group, different meta data). e.g. wrong
         * key group, different meta data.
         */
        ONE_INVALID_STATE_HANDLE(2) {
            @Override
            public OperatorSubtaskState createAlternativeSubtaskState(
                    OperatorSubtaskState primaryOriginal) {
                KeyGroupRange otherRange = new KeyGroupRange(8, 16);
                int numNamedStates = 2;
                return OperatorSubtaskState.builder()
                        .setManagedOperatorState(
                                createNewOperatorStateHandle(numNamedStates, RANDOM))
                        .setRawOperatorState(createNewOperatorStateHandle(numNamedStates, RANDOM))
                        .setManagedKeyedState(createNewKeyedStateHandle(otherRange))
                        .setRawKeyedState(createNewKeyedStateHandle(otherRange))
                        .setInputChannelState(
                                singleton(createNewInputChannelStateHandle(10, RANDOM)))
                        .setResultSubpartitionState(
                                singleton(createNewResultSubpartitionStateHandle(10, RANDOM)))
                        .build();
            }
        };

        CreateAltSubtaskStateMode(int code) {
            this.code = code;
        }

        private final int code;

        static CreateAltSubtaskStateMode byCode(int code) {
            for (CreateAltSubtaskStateMode v : values()) {
                if (v.code == code) {
                    return v;
                }
            }
            throw new IllegalArgumentException("unknown code: " + code);
        }

        public abstract OperatorSubtaskState createAlternativeSubtaskState(
                OperatorSubtaskState primaryOriginal);
    }

    private <T extends StateObject> boolean checkResultAsExpected(
            Function<OperatorSubtaskState, StateObjectCollection<T>> extractor,
            Function<PrioritizedOperatorSubtaskState, List<StateObjectCollection<T>>> extractor2,
            PrioritizedOperatorSubtaskState prioritizedResult,
            OperatorSubtaskState... expectedOrdered) {

        List<StateObjectCollection<T>> collector = new ArrayList<>(expectedOrdered.length);
        for (OperatorSubtaskState operatorSubtaskState : expectedOrdered) {
            collector.add(extractor.apply(operatorSubtaskState));
        }

        return checkRepresentSameOrder(
                extractor2.apply(prioritizedResult).iterator(),
                collector.toArray(new StateObjectCollection[0]));
    }

    private boolean checkRepresentSameOrder(
            Iterator<? extends StateObjectCollection<?>> ordered,
            StateObjectCollection<?>... expectedOrder) {

        for (StateObjectCollection<?> objects : expectedOrder) {
            if (!ordered.hasNext()
                    || !checkContainedObjectsReferentialEquality(objects, ordered.next())) {
                return false;
            }
        }

        return !ordered.hasNext();
    }

    /**
     * Returns true iff, in iteration order, all objects in the first collection are equal by
     * reference to their corresponding object (by order) in the second collection and the size of
     * the collections is equal.
     */
    public boolean checkContainedObjectsReferentialEquality(
            StateObjectCollection<?> a, StateObjectCollection<?> b) {

        if (a == b) {
            return true;
        }

        if (a == null || b == null) {
            return false;
        }

        if (a.size() != b.size()) {
            return false;
        }

        Iterator<?> bIter = b.iterator();
        for (StateObject stateObject : a) {
            if (!bIter.hasNext() || bIter.next() != stateObject) {
                return false;
            }
        }
        return true;
    }

    /**
     * Creates a deep copy of the first state object in the given collection, or null if the
     * collection is empy.
     */
    private static <T extends StateObject> StateObjectCollection<T> deepCopyFirstElement(
            StateObjectCollection<T> original) {
        if (original.isEmpty()) {
            return StateObjectCollection.empty();
        }
        return StateObjectCollection.singleton(deepCopy(original.iterator().next()));
    }

    /**
     * Creates a deep copy of the first state object in the given collection, or null if the
     * collection is empy.
     */
    private static <T extends StateObject> StateObjectCollection<T> deepCopy(
            StateObjectCollection<T> original) {
        if (original == null || original.isEmpty()) {
            return StateObjectCollection.empty();
        }
        return new StateObjectCollection<>(
                original.stream()
                        .map(PrioritizedOperatorSubtaskStateTest::deepCopy)
                        .collect(Collectors.toList()));
    }

    @SuppressWarnings("unchecked")
    private static <T extends StateObject> T deepCopy(T stateObject) {
        if (stateObject instanceof OperatorStreamStateHandle) {
            return (T) deepDummyCopy((OperatorStateHandle) stateObject);
        } else if (stateObject instanceof KeyedStateHandle) {
            return (T) deepDummyCopy((KeyedStateHandle) stateObject);
        } else if (stateObject instanceof InputChannelStateHandle) {
            return (T) deepDummyCopy((InputChannelStateHandle) stateObject);
        } else if (stateObject instanceof ResultSubpartitionStateHandle) {
            return (T) deepDummyCopy((ResultSubpartitionStateHandle) stateObject);
        } else {
            throw new IllegalStateException();
        }
    }

    private <T extends StateObject, ID> StateObjectCollection<T> computeExpectedMixedState(
            List<OperatorSubtaskState> orderedAlternativesList,
            OperatorSubtaskState primaryAndFallback,
            Function<OperatorSubtaskState, StateObjectCollection<T>> stateExtractor,
            Function<T, ID> idExtractor) {

        List<OperatorSubtaskState> reverseAlternatives = new ArrayList<>(orderedAlternativesList);
        Collections.reverse(reverseAlternatives);

        Map<ID, T> map =
                stateExtractor.apply(primaryAndFallback).stream()
                        .collect(Collectors.toMap(idExtractor, Function.identity()));

        reverseAlternatives.stream()
                .flatMap(x -> stateExtractor.apply(x).stream())
                .forEach(x -> map.replace(idExtractor.apply(x), x));

        return new StateObjectCollection<>(map.values());
    }

    static <SH extends StateObject> void assertResultAsExpected(
            StateObjectCollection<SH> expected,
            StateObjectCollection<SH> primary,
            List<StateObjectCollection<SH>> actual) {
        Assertions.assertTrue(!actual.isEmpty() && actual.size() <= 2);
        Assertions.assertTrue(isSameContentUnordered(expected, actual.get(0)));
        if (actual.size() == 1) {
            Assertions.assertTrue(isSameContentUnordered(primary, actual.get(0)));
        } else {
            Assertions.assertTrue(isSameContentUnordered(primary, actual.get(1)));
        }
    }

    static <T> boolean isSameContentUnordered(Collection<T> a, Collection<T> b) {
        return a.size() == b.size() && a.containsAll(b);
    }
}
