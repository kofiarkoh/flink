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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Animal;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Dog;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.DogKryoSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.DogV2KryoSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Parrot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.testutils.ClassLoaderUtils;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.io.Serializable;

import static org.apache.flink.api.common.typeutils.TypeSerializerConditions.isCompatibleAsIs;
import static org.apache.flink.api.common.typeutils.TypeSerializerConditions.isCompatibleWithReconfiguredSerializer;
import static org.apache.flink.api.common.typeutils.TypeSerializerConditions.isIncompatible;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KryoSerializerSnapshot}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class KryoSerializerSnapshotTest {

    private SerializerConfigImpl oldConfig;
    private SerializerConfigImpl newConfig;

    @BeforeEach
    void setup() {
        oldConfig = new SerializerConfigImpl();
        newConfig = new SerializerConfigImpl();
    }

    @Test
    void sanityTest() {
        assertThat(resolveKryoCompatibility(oldConfig, newConfig)).is(isCompatibleAsIs());
    }

    @Test
    void addingTypesIsCompatibleAfterReconfiguration() {
        oldConfig.registerKryoType(Animal.class);

        newConfig.registerKryoType(Animal.class);
        newConfig.registerTypeWithKryoSerializer(Dog.class, DogKryoSerializer.class);

        assertThat(resolveKryoCompatibility(oldConfig, newConfig))
                .is(isCompatibleWithReconfiguredSerializer());
    }

    @Test
    void replacingKryoSerializersIsCompatibleAsIs() {
        oldConfig.registerKryoType(Animal.class);
        oldConfig.registerTypeWithKryoSerializer(Dog.class, DogKryoSerializer.class);

        newConfig.registerKryoType(Animal.class);
        newConfig.registerTypeWithKryoSerializer(Dog.class, DogV2KryoSerializer.class);

        // it is compatible as is, since Kryo does not expose compatibility API with KryoSerializers
        // so we can not know if DogKryoSerializer is compatible with DogV2KryoSerializer
        assertThat(resolveKryoCompatibility(oldConfig, newConfig)).is(isCompatibleAsIs());
    }

    @Test
    void reorderingIsCompatibleAfterReconfiguration() {
        oldConfig.registerKryoType(Parrot.class);
        oldConfig.registerKryoType(Dog.class);

        newConfig.registerKryoType(Dog.class);
        newConfig.registerKryoType(Parrot.class);

        assertThat(resolveKryoCompatibility(oldConfig, newConfig))
                .is(isCompatibleWithReconfiguredSerializer());
    }

    @Test
    void tryingToRestoreWithNonExistingClassShouldBeIncompatible() throws IOException {
        TypeSerializerSnapshot<Animal> restoredSnapshot = kryoSnapshotWithMissingClass();

        TypeSerializer<Animal> currentSerializer =
                new KryoSerializer<>(Animal.class, new SerializerConfigImpl());

        assertThat(
                        currentSerializer
                                .snapshotConfiguration()
                                .resolveSchemaCompatibility(restoredSnapshot))
                .is(isIncompatible());
    }

    // -------------------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------------------

    private static TypeSerializerSnapshot<Animal> kryoSnapshotWithMissingClass()
            throws IOException {
        DataInputView in = new DataInputDeserializer(unLoadableSnapshotBytes());

        return TypeSerializerSnapshot.readVersionedSnapshot(
                in, KryoSerializerSnapshotTest.class.getClassLoader());
    }

    /**
     * This method returns the bytes of a serialized {@link KryoSerializerSnapshot}, that contains a
     * Kryo registration of a class that does not exist in the current classpath.
     */
    private static byte[] unLoadableSnapshotBytes() throws IOException {
        final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();

        final ClassLoaderUtils.ObjectAndClassLoader<Serializable> outsideClassLoading =
                ClassLoaderUtils.createSerializableObjectFromNewClassLoader();

        try {
            Thread.currentThread().setContextClassLoader(outsideClassLoading.getClassLoader());

            SerializerConfigImpl conf = new SerializerConfigImpl();
            conf.registerKryoType(outsideClassLoading.getObject().getClass());

            KryoSerializer<Animal> previousSerializer = new KryoSerializer<>(Animal.class, conf);
            TypeSerializerSnapshot<Animal> previousSnapshot =
                    previousSerializer.snapshotConfiguration();

            DataOutputSerializer out = new DataOutputSerializer(4096);
            TypeSerializerSnapshot.writeVersionedSnapshot(out, previousSnapshot);
            return out.getCopyOfBuffer();
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    private static TypeSerializerSchemaCompatibility<Animal> resolveKryoCompatibility(
            SerializerConfigImpl previous, SerializerConfigImpl current) {
        KryoSerializer<Animal> previousSerializer = new KryoSerializer<>(Animal.class, previous);
        TypeSerializerSnapshot<Animal> previousSnapshot =
                previousSerializer.snapshotConfiguration();

        TypeSerializer<Animal> currentSerializer = new KryoSerializer<>(Animal.class, current);
        return currentSerializer
                .snapshotConfiguration()
                .resolveSchemaCompatibility(previousSnapshot);
    }
}
