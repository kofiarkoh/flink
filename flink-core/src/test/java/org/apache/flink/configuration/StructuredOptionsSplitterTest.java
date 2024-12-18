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

import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StructuredOptionsSplitter}. */
@ExtendWith(ParameterizedTestExtension.class)
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class StructuredOptionsSplitterTest {

    @Parameters(name = "testSpec = {0}")
    private static Collection<TestSpec> getSpecs() {
        return Arrays.asList(

                // Use single quotes for quoting
                TestSpec.split("'A;B';C", ';').expect("A;B", "C"),
                TestSpec.split("'A;B';'C'", ';').expect("A;B", "C"),
                TestSpec.split("A;B;C", ';').expect("A", "B", "C"),
                TestSpec.split("'AB''D;B';C", ';').expect("AB'D;B", "C"),
                TestSpec.split("A'BD;B';C", ';').expect("A'BD", "B'", "C"),
                TestSpec.split("'AB'D;B;C", ';')
                        .expectException("Could not split string. Illegal quoting at position: 3"),
                TestSpec.split("'A", ';')
                        .expectException(
                                "Could not split string. Quoting was not closed properly."),
                TestSpec.split("C;'", ';')
                        .expectException(
                                "Could not split string. Quoting was not closed properly."),

                // Use double quotes for quoting
                TestSpec.split("\"A;B\";C", ';').expect("A;B", "C"),
                TestSpec.split("\"A;B\";\"C\"", ';').expect("A;B", "C"),
                TestSpec.split("A;B;C", ';').expect("A", "B", "C"),
                TestSpec.split("\"AB\"\"D;B\";C", ';').expect("AB\"D;B", "C"),
                TestSpec.split("A\"BD;B\";C", ';').expect("A\"BD", "B\"", "C"),
                TestSpec.split("\"AB\"D;B;C", ';')
                        .expectException("Could not split string. Illegal quoting at position: 3"),
                TestSpec.split("\"A", ';')
                        .expectException(
                                "Could not split string. Quoting was not closed properly."),
                TestSpec.split("C;\"", ';')
                        .expectException(
                                "Could not split string. Quoting was not closed properly."),

                // Mix different quoting
                TestSpec.split("'AB\"D';B;C", ';').expect("AB\"D", "B", "C"),
                TestSpec.split("'AB\"D;B';C", ';').expect("AB\"D;B", "C"),
                TestSpec.split("'AB\"''D;B';C", ';').expect("AB\"'D;B", "C"),
                TestSpec.split("\"AB'D\";B;C", ';').expect("AB'D", "B", "C"),
                TestSpec.split("\"AB'D;B\";C", ';').expect("AB'D;B", "C"),
                TestSpec.split("\"AB'\"\"D;B\";C", ';').expect("AB'\"D;B", "C"),

                // Use different delimiter
                TestSpec.split("'A,B',C", ',').expect("A,B", "C"),
                TestSpec.split("A,B,C", ',').expect("A", "B", "C"),

                // Whitespaces handling
                TestSpec.split("   'A;B'    ;   C   ", ';').expect("A;B", "C"),
                TestSpec.split("   A;B    ;   C   ", ';').expect("A", "B", "C"),
                TestSpec.split("'A;B'    ;C A", ';').expect("A;B", "C A"),
                TestSpec.split("' A    ;B'    ;'   C'", ';').expect(" A    ;B", "   C"));
    }

    @Parameter private TestSpec testSpec;

    @TestTemplate
    void testParse() {

        Optional<String> expectedException = testSpec.getExpectedException();
        if (expectedException.isPresent()) {
            assertThatThrownBy(
                            () ->
                                    StructuredOptionsSplitter.splitEscaped(
                                            testSpec.getString(), testSpec.getDelimiter()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(expectedException.get());
            return;
        }
        List<String> splits =
                StructuredOptionsSplitter.splitEscaped(
                        testSpec.getString(), testSpec.getDelimiter());

        assertThat(splits).isEqualTo(testSpec.getExpectedSplits());
    }

    private static class TestSpec {
        private final String string;
        private final char delimiter;
        @Nullable private String expectedException = null;
        private List<String> expectedSplits = null;

        private TestSpec(String string, char delimiter) {
            this.string = string;
            this.delimiter = delimiter;
        }

        public static TestSpec split(String string, char delimiter) {
            return new TestSpec(string, delimiter);
        }

        public TestSpec expect(String... splits) {
            this.expectedSplits = Arrays.asList(splits);
            return this;
        }

        public TestSpec expectException(String message) {
            this.expectedException = message;
            return this;
        }

        public String getString() {
            return string;
        }

        public char getDelimiter() {
            return delimiter;
        }

        public Optional<String> getExpectedException() {
            return Optional.ofNullable(expectedException);
        }

        public List<String> getExpectedSplits() {
            return expectedSplits;
        }

        @Override
        public String toString() {
            return String.format(
                    "str = [ %s ], del = '%s', expected = %s",
                    string,
                    delimiter,
                    getExpectedException()
                            .map(e -> String.format("Exception(%s)", e))
                            .orElseGet(
                                    () ->
                                            expectedSplits.stream()
                                                    .collect(
                                                            Collectors.joining("], [", "[", "]"))));
        }
    }
}
