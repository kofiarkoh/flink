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

package org.apache.flink.configuration;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ConfigOption}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class ConfigOptionTest {

    @Test
    void testDeprecationFlagForDeprecatedKeys() {
        final ConfigOption<Integer> optionWithDeprecatedKeys =
                ConfigOptions.key("key")
                        .intType()
                        .defaultValue(0)
                        .withDeprecatedKeys("deprecated1", "deprecated2");

        assertThat(optionWithDeprecatedKeys.hasFallbackKeys()).isTrue();
        for (final FallbackKey fallbackKey : optionWithDeprecatedKeys.fallbackKeys()) {
            assertThat(fallbackKey.isDeprecated())
                    .as("deprecated key not flagged as deprecated")
                    .isTrue();
        }
    }

    @Test
    void testDeprecationFlagForFallbackKeys() {
        final ConfigOption<Integer> optionWithFallbackKeys =
                ConfigOptions.key("key")
                        .intType()
                        .defaultValue(0)
                        .withFallbackKeys("fallback1", "fallback2");

        assertThat(optionWithFallbackKeys.hasFallbackKeys()).isTrue();
        for (final FallbackKey fallbackKey : optionWithFallbackKeys.fallbackKeys()) {
            assertThat(fallbackKey.isDeprecated())
                    .as("falback key flagged as deprecated")
                    .isFalse();
        }
    }

    @Test
    void testDeprecationFlagForMixedAlternativeKeys() {
        final ConfigOption<Integer> optionWithMixedKeys =
                ConfigOptions.key("key")
                        .intType()
                        .defaultValue(0)
                        .withDeprecatedKeys("deprecated1", "deprecated2")
                        .withFallbackKeys("fallback1", "fallback2");

        final List<String> fallbackKeys = new ArrayList<>(2);
        final List<String> deprecatedKeys = new ArrayList<>(2);
        for (final FallbackKey alternativeKey : optionWithMixedKeys.fallbackKeys()) {
            if (alternativeKey.isDeprecated()) {
                deprecatedKeys.add(alternativeKey.getKey());
            } else {
                fallbackKeys.add(alternativeKey.getKey());
            }
        }

        assertThat(fallbackKeys).hasSize(2);
        assertThat(deprecatedKeys).hasSize(2);

        assertThat(fallbackKeys).containsExactlyInAnyOrder("fallback1", "fallback2");
        assertThat(deprecatedKeys).containsExactlyInAnyOrder("deprecated1", "deprecated2");
    }
}
