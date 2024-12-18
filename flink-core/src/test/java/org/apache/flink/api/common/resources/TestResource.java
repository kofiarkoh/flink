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

package org.apache.flink.api.common.resources;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.math.BigDecimal;

/** Test implementation for {@link Resource}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class TestResource extends Resource<TestResource> {

    public static final String NAME = "TestResource";

    public TestResource(final double value) {
        super(NAME, value);
    }

    public TestResource(final BigDecimal value) {
        super(NAME, value);
    }

    public TestResource(final String name, final double value) {
        super(name, value);
    }

    @Override
    public TestResource create(BigDecimal value) {
        return new TestResource(value);
    }
}
