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

package org.apache.flink.api.common.operators.util;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class FieldListTest {

    @Test
    void testFieldListConstructors() {
        check(new FieldList());
        check(FieldList.EMPTY_LIST);
        check(new FieldList(14), 14);
        check(new FieldList(Integer.valueOf(3)), 3);
        check(new FieldList(7, 4, 1), 7, 4, 1);
        check(new FieldList(7, 4, 1, 4, 7, 1, 4, 2), 7, 4, 1, 4, 7, 1, 4, 2);
    }

    @Test
    void testFieldListAdds() {
        check(new FieldList().addField(1).addField(2), 1, 2);
        check(FieldList.EMPTY_LIST.addField(3).addField(2), 3, 2);
        check(new FieldList(13).addFields(new FieldList(17, 31, 42)), 13, 17, 31, 42);
        check(new FieldList(14).addFields(new FieldList(17)), 14, 17);
        check(new FieldList(3).addFields(2, 8, 5, 7), 3, 2, 8, 5, 7);
    }

    @Test
    void testImmutability() {
        FieldList s1 = new FieldList();
        FieldList s2 = new FieldList(5);
        FieldList s3 = new FieldList(Integer.valueOf(7));
        FieldList s4 = new FieldList(5, 4, 7, 6);

        s1.addFields(s2).addFields(s3);
        s2.addFields(s4);
        s4.addFields(s1);

        s1.addField(Integer.valueOf(14));
        s2.addFields(78, 13, 66, 3);

        assertThat(s1).isEmpty();
        assertThat(s2).hasSize(1);
        assertThat(s3).hasSize(1);
        assertThat(s4).hasSize(4);
    }

    @Test
    void testAddSetToList() {
        check(new FieldList().addFields(new FieldSet(1)).addFields(2), 1, 2);
        check(new FieldList().addFields(1).addFields(new FieldSet(2)), 1, 2);
        check(new FieldList().addFields(new FieldSet(2)), 2);
    }

    private static void check(FieldList set, int... elements) {
        if (elements == null) {
            assertThat(set).isEmpty();
            return;
        }

        assertThat(set).hasSameSizeAs(elements);

        // test contains
        for (int i : elements) {
            set.contains(i);
        }

        // test to array
        {
            int[] arr = set.toArray();
            assertThat(elements).isEqualTo(arr);
        }

        {
            int[] fromIter = new int[set.size()];
            Iterator<Integer> iter = set.iterator();

            for (int i = 0; i < fromIter.length; i++) {
                fromIter[i] = iter.next();
            }
            assertThat(iter).isExhausted();
            assertThat(elements).isEqualTo(fromIter);
        }
    }
}
