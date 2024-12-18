/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.module.hive;
import edu.illinois.CTestJUnit5Extension;

import org.junit.jupiter.api.extension.ExtendWith;

import edu.illinois.CTestClass;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.module.Module;

import org.junit.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HiveModuleFactory}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class HiveModuleFactoryTest {
    @Test
    public void test() {
        final HiveModule expected = new HiveModule();

        final Module actualModule =
                FactoryUtil.createModule(
                        HiveModuleFactory.IDENTIFIER,
                        Collections.emptyMap(),
                        new Configuration(),
                        Thread.currentThread().getContextClassLoader());

        checkEquals(expected, (HiveModule) actualModule);
    }

    private static void checkEquals(HiveModule m1, HiveModule m2) {
        assertThat(m2.getHiveVersion()).isEqualTo(m1.getHiveVersion());
    }
}
