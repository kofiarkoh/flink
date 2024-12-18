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

package org.apache.flink.util;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link IterableUtils}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class IterableUtilsTest {

    private final Iterable<Integer> testIterable = Arrays.asList(1, 8, 5, 3, 8);

    @Test
    void testToStream() {
        Queue<Integer> deque = new ArrayDeque<>();
        testIterable.forEach(deque::add);

        Stream<Integer> stream = IterableUtils.toStream(testIterable);
        assertThat(stream.allMatch(value -> deque.poll().equals(value))).isTrue();
    }
}
