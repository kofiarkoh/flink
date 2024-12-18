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

package org.apache.flink.formats.avro.typeutils;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.formats.avro.generated.User;
import org.apache.flink.formats.avro.utils.TestDataGenerator;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Random;

/** Tests for the {@link AvroSerializer} that test specific avro types. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class AvroSerializerTest extends SerializerTestBase<User> {

    @Override
    protected TypeSerializer<User> createSerializer() {
        return new AvroSerializer<>(User.class);
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<User> getTypeClass() {
        return User.class;
    }

    @Override
    protected User[] getTestData() {
        final Random rnd = new Random();
        final User[] users = new User[20];

        for (int i = 0; i < users.length; i++) {
            users[i] = TestDataGenerator.generateRandomUser(rnd);
        }

        return users;
    }
}
