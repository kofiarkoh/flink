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

package org.apache.flink.api.common.state;

import org.apache.flink.api.common.typeinfo.TypeDescriptors;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MapStateDeclaration} and {@link BroadcastStateDeclaration}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class MapStateDeclarationTest {

    @Test
    void testMapStateDeclarationName() {
        MapStateDeclaration<Integer, String> mapStateDeclaration =
                StateDeclarations.mapStateBuilder(
                                "mapState", TypeDescriptors.INT, TypeDescriptors.STRING)
                        .build();
        assertThat(mapStateDeclaration.getName()).isEqualTo("mapState");
    }

    @Test
    void testMapStateDeclarationType() {
        MapStateDeclaration<Integer, String> mapStateDeclaration =
                StateDeclarations.mapStateBuilder(
                                "mapState", TypeDescriptors.INT, TypeDescriptors.STRING)
                        .build();
        assertThat(mapStateDeclaration.getKeyTypeDescriptor()).isEqualTo(TypeDescriptors.INT);
        assertThat(mapStateDeclaration.getValueTypeDescriptor()).isEqualTo(TypeDescriptors.STRING);
    }

    @Test
    void testMapStateDeclarationRedistribution() {
        MapStateDeclaration<Integer, String> mapStateDeclaration =
                StateDeclarations.mapStateBuilder(
                                "mapState", TypeDescriptors.INT, TypeDescriptors.STRING)
                        .build();
        assertThat(mapStateDeclaration.getRedistributionMode())
                .isEqualTo(StateDeclaration.RedistributionMode.NONE);
    }

    @Test
    void testBroadcastStateDeclaration() {
        BroadcastStateDeclaration<Integer, String> broadcastState =
                StateDeclarations.mapStateBuilder(
                                "broadcastState", TypeDescriptors.INT, TypeDescriptors.STRING)
                        .buildBroadcast();
        assertThat(broadcastState.getRedistributionMode())
                .isEqualTo(StateDeclaration.RedistributionMode.IDENTICAL);
        assertThat(broadcastState.getName()).isEqualTo("broadcastState");
        assertThat(broadcastState.getKeyTypeDescriptor()).isEqualTo(TypeDescriptors.INT);
        assertThat(broadcastState.getValueTypeDescriptor()).isEqualTo(TypeDescriptors.STRING);
    }
}
