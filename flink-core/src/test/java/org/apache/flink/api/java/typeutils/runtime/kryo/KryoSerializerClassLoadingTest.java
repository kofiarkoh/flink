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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.testutils.ClassLoaderUtils;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.Serializable;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * This test validates that the Kryo-based serializer handles classes with custom class loaders
 * correctly.
 */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class KryoSerializerClassLoadingTest extends SerializerTestBase<Object> {

    /**
     * @ExtendWith(CTestJUnit5Extension.class) @CTestClass Class loader and object that is not in
     * the test class path.
     */
    private static final ClassLoaderUtils.ObjectAndClassLoader<Serializable> OUTSIDE_CLASS_LOADING =
            ClassLoaderUtils.createSerializableObjectFromNewClassLoader();

    // ------------------------------------------------------------------------

    private ClassLoader originalClassLoader;

    @BeforeEach
    void setupClassLoader() {
        originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(OUTSIDE_CLASS_LOADING.getClassLoader());
    }

    @AfterEach
    void restoreOriginalClassLoader() {
        Thread.currentThread().setContextClassLoader(originalClassLoader);
    }

    // ------------------------------------------------------------------------

    @Test
    void guardTestAssumptions() {
        assertThatThrownBy(
                        () -> Class.forName(OUTSIDE_CLASS_LOADING.getObject().getClass().getName()))
                .isInstanceOf(ClassNotFoundException.class)
                .withFailMessage("This test's assumptions are broken");
    }

    // ------------------------------------------------------------------------

    @Override
    protected TypeSerializer<Object> createSerializer() {
        return new KryoSerializer<>(Object.class, new SerializerConfigImpl());
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<Object> getTypeClass() {
        return Object.class;
    }

    @Override
    protected Object[] getTestData() {
        return new Object[] {
            new Integer(7),

            // an object whose class is not on the classpath
            OUTSIDE_CLASS_LOADING.getObject(),

            // an object whose class IS on the classpath with a nested object whose class
            // is NOT on the classpath
            new Tuple1<>(OUTSIDE_CLASS_LOADING.getObject())
        };
    }

    @Override
    public void testInstantiate() {
        // this serializer does not support instantiation
    }
}
