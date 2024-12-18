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

package org.apache.flink.core.fs;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the {@link AutoCloseableRegistry}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class AutoCloseableRegistryTest {
    @Test
    void testReverseOrderOfClosing() throws Exception {
        ArrayList<Integer> closeOrder = new ArrayList<>();
        AutoCloseableRegistry autoCloseableRegistry = new AutoCloseableRegistry();
        autoCloseableRegistry.registerCloseable(() -> closeOrder.add(3));
        autoCloseableRegistry.registerCloseable(() -> closeOrder.add(2));
        autoCloseableRegistry.registerCloseable(() -> closeOrder.add(1));

        autoCloseableRegistry.close();

        int expected = 1;
        for (int actual : closeOrder) {
            assertThat(actual).isEqualTo(expected++);
        }
    }

    @Test
    void testSuppressedExceptions() throws Exception {
        AutoCloseableRegistry autoCloseableRegistry = new AutoCloseableRegistry();
        autoCloseableRegistry.registerCloseable(
                () -> {
                    throw new AssertionError("3");
                });
        autoCloseableRegistry.registerCloseable(
                () -> {
                    throw new Exception("2");
                });
        autoCloseableRegistry.registerCloseable(
                () -> {
                    throw new Exception("1");
                });

        try {
            autoCloseableRegistry.close();

            fail("Close should throw exception");
        } catch (Exception ex) {
            assertThatThrownBy(
                            () -> {
                                throw ex;
                            })
                    .hasMessage("1")
                    .satisfies(
                            e -> assertThat(e.getSuppressed()[0]).hasMessage("2"),
                            e ->
                                    assertThat(e.getSuppressed()[1])
                                            .hasMessage("java.lang.AssertionError: 3"));
        }
    }
}
