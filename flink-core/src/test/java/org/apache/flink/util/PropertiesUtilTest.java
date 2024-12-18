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

import java.util.Properties;

import static org.apache.flink.util.PropertiesUtil.flatten;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link PropertiesUtil}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class PropertiesUtilTest {

    @Test
    void testFlatten() {
        // default Properties is null
        Properties prop1 = new Properties();
        prop1.put("key1", "value1");

        // default Properties is prop1
        Properties prop2 = new Properties(prop1);
        prop2.put("key2", "value2");

        // default Properties is prop2
        Properties prop3 = new Properties(prop2);
        prop3.put("key3", "value3");

        Properties flattened = flatten(prop3);
        assertThat("value1").isEqualTo(flattened.get("key1"));
        assertThat("value2").isEqualTo(flattened.get("key2"));
        assertThat("value3").isEqualTo(flattened.get("key3"));
    }
}
