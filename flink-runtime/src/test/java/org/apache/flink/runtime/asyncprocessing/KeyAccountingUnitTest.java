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

package org.apache.flink.runtime.asyncprocessing;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test for {@link KeyAccountingUnit}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class KeyAccountingUnitTest {

    @Test
    void testBasic() {
        KeyAccountingUnit<Integer> keyAccountingUnit = new KeyAccountingUnit<>(10);
        assertThat(keyAccountingUnit.occupy("record1", 1)).isTrue();
        assertThat(keyAccountingUnit.occupy("record1", 1)).isFalse();
        assertThat(keyAccountingUnit.occupy("record2", 2)).isTrue();
        keyAccountingUnit.release("record1", 1);
        assertThat(keyAccountingUnit.occupy("record2", 1)).isTrue();
        assertThatThrownBy(() -> keyAccountingUnit.release("record1", 1))
                .isInstanceOf(IllegalStateException.class);
    }
}
