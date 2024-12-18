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

package org.apache.flink.runtime.operators.hash;

import org.apache.flink.api.common.typeutils.SameTypePairComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongComparator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.operators.testutils.types.IntList;
import org.apache.flink.runtime.operators.testutils.types.IntPair;
import org.apache.flink.util.MutableObjectIterator;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class CompactingHashTableTest extends MutableHashTableTestBase {

    private final TypeComparator<Long> probeComparator;

    private final TypePairComparator<Long, Tuple2<Long, String>> pairComparator;

    public CompactingHashTableTest() {

        this.probeComparator = new LongComparator(true);

        this.pairComparator =
                new TypePairComparator<Long, Tuple2<Long, String>>() {

                    private long ref;

                    @Override
                    public void setReference(Long reference) {
                        ref = reference;
                    }

                    @Override
                    public boolean equalToReference(Tuple2<Long, String> candidate) {
                        //noinspection UnnecessaryUnboxing
                        return candidate.f0.longValue() == ref;
                    }

                    @Override
                    public int compareToReference(Tuple2<Long, String> candidate) {
                        long x = ref;
                        long y = candidate.f0;
                        return (x < y) ? -1 : ((x == y) ? 0 : 1);
                    }
                };
    }

    @Override
    protected <T> AbstractMutableHashTable<T> getHashTable(
            TypeSerializer<T> serializer,
            TypeComparator<T> comparator,
            List<MemorySegment> memory) {
        return new CompactingHashTable<T>(serializer, comparator, memory);
    }

    // ------------------------------------------------------------------------
    //  tests
    // ------------------------------------------------------------------------

    /**
     * This has to be duplicated in InPlaceMutableHashTableTest and CompactingHashTableTest because
     * of the different constructor calls.
     */
    @Test
    void testHashTableGrowthWithInsert() {
        try {
            final int numElements = 1000000;

            List<MemorySegment> memory = getMemory(10000, 32 * 1024);

            // we create a hash table that thinks the records are super large. that makes it choose
            // initially
            // a lot of memory for the partition buffers, and start with a smaller hash table. that
            // way
            // we trigger a hash table growth early.
            CompactingHashTable<Tuple2<Long, String>> table =
                    new CompactingHashTable<Tuple2<Long, String>>(
                            tuple2LongStringSerializer, tuple2LongStringComparator, memory, 10000);
            table.open();

            for (long i = 0; i < numElements; i++) {
                table.insert(new Tuple2<Long, String>(i, String.valueOf(i)));
            }

            // make sure that all elements are contained via the entry iterator
            {
                BitSet bitSet = new BitSet(numElements);
                MutableObjectIterator<Tuple2<Long, String>> iter = table.getEntryIterator();
                Tuple2<Long, String> next;
                while ((next = iter.next()) != null) {
                    assertThat(next.f1).isNotNull();
                    assertThat(next.f0).isNotNull().isEqualTo(Long.parseLong(next.f1));

                    bitSet.set(next.f0.intValue());
                }

                assertThat(bitSet.cardinality()).isEqualTo(numElements);
            }

            // make sure all entries are contained via the prober
            {
                CompactingHashTable<Tuple2<Long, String>>.HashTableProber<Long> proper =
                        table.getProber(probeComparator, pairComparator);

                for (long i = 0; i < numElements; i++) {
                    assertThat(proper.getMatchFor(i)).isNotNull();
                    assertThat(proper.getMatchFor(i + numElements)).isNull();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /**
     * This test validates that records are not lost via "insertOrReplace()" as in bug [FLINK-2361]
     *
     * <p>This has to be duplicated in InPlaceMutableHashTableTest and CompactingHashTableTest
     * because of the different constructor calls.
     */
    @Test
    void testHashTableGrowthWithInsertOrReplace() {
        try {
            final int numElements = 1000000;

            List<MemorySegment> memory = getMemory(10000, 32 * 1024);

            // we create a hash table that thinks the records are super large. that makes it choose
            // initially
            // a lot of memory for the partition buffers, and start with a smaller hash table. that
            // way
            // we trigger a hash table growth early.
            CompactingHashTable<Tuple2<Long, String>> table =
                    new CompactingHashTable<>(
                            tuple2LongStringSerializer, tuple2LongStringComparator, memory, 10000);
            table.open();

            for (long i = 0; i < numElements; i++) {
                table.insertOrReplaceRecord(new Tuple2<Long, String>(i, String.valueOf(i)));
            }

            // make sure that all elements are contained via the entry iterator
            {
                BitSet bitSet = new BitSet(numElements);
                MutableObjectIterator<Tuple2<Long, String>> iter = table.getEntryIterator();
                Tuple2<Long, String> next;
                while ((next = iter.next()) != null) {
                    assertThat(next.f1).isNotNull();
                    assertThat(next.f0).isNotNull().isEqualTo(Long.parseLong(next.f1));

                    bitSet.set(next.f0.intValue());
                }

                assertThat(bitSet.cardinality()).isEqualTo(numElements);
            }

            // make sure all entries are contained via the prober
            {
                CompactingHashTable<Tuple2<Long, String>>.HashTableProber<Long> proper =
                        table.getProber(probeComparator, pairComparator);

                for (long i = 0; i < numElements; i++) {
                    assertThat(proper.getMatchFor(i)).isNotNull();
                    assertThat(proper.getMatchFor(i + numElements)).isNull();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /**
     * This test validates that new inserts (rather than updates) in "insertOrReplace()" properly
     * react to out of memory conditions.
     */
    @Test
    void testInsertsWithInsertOrReplace() {
        try {
            final int numElements = 1000;

            final String longString = getLongString(10000);
            List<MemorySegment> memory = getMemory(1000, 32 * 1024);

            // we create a hash table that thinks the records are super large. that makes it choose
            // initially
            // a lot of memory for the partition buffers, and start with a smaller hash table. that
            // way
            // we trigger a hash table growth early.
            CompactingHashTable<Tuple2<Long, String>> table =
                    new CompactingHashTable<>(
                            tuple2LongStringSerializer, tuple2LongStringComparator, memory, 100);
            table.open();

            // first, we insert some elements
            for (long i = 0; i < numElements; i++) {
                table.insertOrReplaceRecord(Tuple2.of(i, longString));
            }

            // now, we replace the same elements, causing fragmentation
            for (long i = 0; i < numElements; i++) {
                table.insertOrReplaceRecord(Tuple2.of(i, longString));
            }

            // now we insert an additional set of elements. without compaction during this
            // insertion,
            // the memory will run out
            for (long i = 0; i < numElements; i++) {
                table.insertOrReplaceRecord(Tuple2.of(i + numElements, longString));
            }

            // check the results
            CompactingHashTable<Tuple2<Long, String>>.HashTableProber<Tuple2<Long, String>> prober =
                    table.getProber(
                            tuple2LongStringComparator,
                            new SameTypePairComparator<>(tuple2LongStringComparator));
            Tuple2<Long, String> reuse = new Tuple2<>();
            for (long i = 0; i < numElements; i++) {
                assertThat(prober.getMatchFor(Tuple2.of(i, longString), reuse)).isNotNull();
                assertThat(prober.getMatchFor(Tuple2.of(i + numElements, longString), reuse))
                        .isNotNull();
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testResize() {
        // Only CompactingHashTable
        try {
            final int NUM_MEM_PAGES = 30 * NUM_PAIRS / PAGE_SIZE;
            final Random rnd = new Random(RANDOM_SEED);
            final IntPair[] pairs = getRandomizedIntPairs(NUM_PAIRS, rnd);

            List<MemorySegment> memory = getMemory(NUM_MEM_PAGES);
            CompactingHashTable<IntPair> table =
                    new CompactingHashTable<IntPair>(intPairSerializer, intPairComparator, memory);
            table.open();

            for (int i = 0; i < NUM_PAIRS; i++) {
                table.insert(pairs[i]);
            }

            AbstractHashTableProber<IntPair, IntPair> prober =
                    table.getProber(
                            intPairComparator, new SameTypePairComparator<>(intPairComparator));
            IntPair target = new IntPair();

            for (int i = 0; i < NUM_PAIRS; i++) {
                assertThat(prober.getMatchFor(pairs[i], target)).isNotNull();
                assertThat(pairs[i].getValue()).isEqualTo(target.getValue());
            }

            // make sure there is enough memory for resize
            memory.addAll(getMemory(ADDITIONAL_MEM));
            Boolean b = table.resizeHashTable();
            assertThat(b).isTrue();

            for (int i = 0; i < NUM_PAIRS; i++) {
                assertThat(prober.getMatchFor(pairs[i], target)).isNotNull();
                assertThat(pairs[i].getValue()).isEqualTo(target.getValue());
            }

            table.close();
            assertThat(table.getFreeMemory())
                    .withFailMessage("Memory lost")
                    .hasSize(NUM_MEM_PAGES + ADDITIONAL_MEM);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error: " + e.getMessage());
        }
    }

    @Test
    void testDoubleResize() {
        // Only CompactingHashTable
        try {
            final int NUM_MEM_PAGES = 30 * NUM_PAIRS / PAGE_SIZE;
            final Random rnd = new Random(RANDOM_SEED);
            final IntPair[] pairs = getRandomizedIntPairs(NUM_PAIRS, rnd);

            List<MemorySegment> memory = getMemory(NUM_MEM_PAGES);
            CompactingHashTable<IntPair> table =
                    new CompactingHashTable<IntPair>(intPairSerializer, intPairComparator, memory);
            table.open();

            for (int i = 0; i < NUM_PAIRS; i++) {
                table.insert(pairs[i]);
            }

            AbstractHashTableProber<IntPair, IntPair> prober =
                    table.getProber(
                            intPairComparator, new SameTypePairComparator<>(intPairComparator));
            IntPair target = new IntPair();

            for (int i = 0; i < NUM_PAIRS; i++) {
                assertThat(prober.getMatchFor(pairs[i], target)).isNotNull();
                assertThat(pairs[i].getValue()).isEqualTo(target.getValue());
            }

            // make sure there is enough memory for resize
            memory.addAll(getMemory(ADDITIONAL_MEM));
            Boolean b = table.resizeHashTable();
            assertThat(b).isTrue();

            for (int i = 0; i < NUM_PAIRS; i++) {
                assertThat(prober.getMatchFor(pairs[i], target)).isNotNull();
                assertThat(pairs[i].getValue()).isEqualTo(target.getValue());
            }

            // make sure there is enough memory for resize
            memory.addAll(getMemory(ADDITIONAL_MEM));
            b = table.resizeHashTable();
            assertThat(b).isTrue();

            for (int i = 0; i < NUM_PAIRS; i++) {
                assertThat(prober.getMatchFor(pairs[i], target)).isNotNull();
                assertThat(pairs[i].getValue()).isEqualTo(target.getValue());
            }

            table.close();
            assertThat(table.getFreeMemory())
                    .withFailMessage("Memory lost")
                    .hasSize(NUM_MEM_PAGES + ADDITIONAL_MEM + ADDITIONAL_MEM);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error: " + e.getMessage());
        }
    }

    @Test
    void testTripleResize() {
        // Only CompactingHashTable
        try {
            final int NUM_MEM_PAGES = 30 * NUM_PAIRS / PAGE_SIZE;
            final Random rnd = new Random(RANDOM_SEED);
            final IntPair[] pairs = getRandomizedIntPairs(NUM_PAIRS, rnd);

            List<MemorySegment> memory = getMemory(NUM_MEM_PAGES);
            CompactingHashTable<IntPair> table =
                    new CompactingHashTable<IntPair>(intPairSerializer, intPairComparator, memory);
            table.open();

            for (int i = 0; i < NUM_PAIRS; i++) {
                table.insert(pairs[i]);
            }

            AbstractHashTableProber<IntPair, IntPair> prober =
                    table.getProber(
                            intPairComparator, new SameTypePairComparator<>(intPairComparator));
            IntPair target = new IntPair();

            for (int i = 0; i < NUM_PAIRS; i++) {
                assertThat(prober.getMatchFor(pairs[i], target)).isNotNull();
                assertThat(pairs[i].getValue()).isEqualTo(target.getValue());
            }

            // make sure there is enough memory for resize
            memory.addAll(getMemory(ADDITIONAL_MEM));
            Boolean b = table.resizeHashTable();
            assertThat(b).isTrue();

            for (int i = 0; i < NUM_PAIRS; i++) {
                assertThat(prober.getMatchFor(pairs[i], target)).isNotNull();
                assertThat(pairs[i].getValue()).isEqualTo(target.getValue());
            }

            // make sure there is enough memory for resize
            memory.addAll(getMemory(ADDITIONAL_MEM));
            b = table.resizeHashTable();
            assertThat(b).isTrue();

            for (int i = 0; i < NUM_PAIRS; i++) {
                assertThat(prober.getMatchFor(pairs[i], target)).isNotNull();
                assertThat(pairs[i].getValue()).isEqualTo(target.getValue());
            }

            // make sure there is enough memory for resize
            memory.addAll(getMemory(2 * ADDITIONAL_MEM));
            b = table.resizeHashTable();
            assertThat(b).isTrue();

            for (int i = 0; i < NUM_PAIRS; i++) {
                assertThat(prober.getMatchFor(pairs[i], target)).isNotNull();
                assertThat(pairs[i].getValue()).isEqualTo(target.getValue());
            }

            table.close();
            assertThat(table.getFreeMemory())
                    .withFailMessage("Memory lost")
                    .hasSize(NUM_MEM_PAGES + 4 * ADDITIONAL_MEM);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error: " + e.getMessage());
        }
    }

    @Test
    void testResizeWithCompaction() {
        // Only CompactingHashTable
        try {
            final int NUM_MEM_PAGES = (SIZE * NUM_LISTS / PAGE_SIZE);

            final Random rnd = new Random(RANDOM_SEED);
            final IntList[] lists = getRandomizedIntLists(NUM_LISTS, rnd);

            List<MemorySegment> memory = getMemory(NUM_MEM_PAGES);
            CompactingHashTable<IntList> table =
                    new CompactingHashTable<IntList>(serializerV, comparatorV, memory);
            table.open();

            for (int i = 0; i < NUM_LISTS; i++) {
                table.insert(lists[i]);
            }

            AbstractHashTableProber<IntList, IntList> prober =
                    table.getProber(comparatorV, pairComparatorV);
            IntList target = new IntList();

            for (int i = 0; i < NUM_LISTS; i++) {
                assertThat(prober.getMatchFor(lists[i], target)).isNotNull();
                assertThat(lists[i].getValue()).isEqualTo(target.getValue());
            }

            // make sure there is enough memory for resize
            memory.addAll(getMemory(ADDITIONAL_MEM));
            Boolean b = table.resizeHashTable();
            assertThat(b).isTrue();

            for (int i = 0; i < NUM_LISTS; i++) {
                assertThat(prober.getMatchFor(lists[i], target)).isNotNull();
                assertThat(lists[i].getValue()).isEqualTo(target.getValue());
            }

            final IntList[] overwriteLists = getRandomizedIntLists(NUM_LISTS, rnd);

            // test replacing
            for (int i = 0; i < NUM_LISTS; i++) {
                table.insertOrReplaceRecord(overwriteLists[i]);
            }

            table.compactPartitions();

            // make sure there is enough memory for resize
            memory.addAll(getMemory(2 * ADDITIONAL_MEM));
            b = table.resizeHashTable();
            assertThat(b).isTrue();

            for (int i = 0; i < NUM_LISTS; i++) {
                assertThat(prober.getMatchFor(overwriteLists[i], target)).isNotNull();
                assertThat(overwriteLists[i].getValue()).isEqualTo(target.getValue());
            }

            table.close();
            assertThat(table.getFreeMemory())
                    .withFailMessage("Memory lost")
                    .hasSize(NUM_MEM_PAGES + 3 * ADDITIONAL_MEM);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error: " + e.getMessage());
        }
    }

    private static List<MemorySegment> getMemory(int numSegments, int segmentSize) {
        ArrayList<MemorySegment> list = new ArrayList<MemorySegment>(numSegments);
        for (int i = 0; i < numSegments; i++) {
            list.add(MemorySegmentFactory.allocateUnpooledSegment(segmentSize));
        }
        return list;
    }

    private static String getLongString(int length) {
        StringBuilder bld = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            bld.append('a');
        }
        return bld.toString();
    }
}
