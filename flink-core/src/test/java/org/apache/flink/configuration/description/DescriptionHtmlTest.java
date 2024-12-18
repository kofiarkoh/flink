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

package org.apache.flink.configuration.description;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.apache.flink.configuration.description.LinkElement.link;
import static org.apache.flink.configuration.description.TextElement.text;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link Description} and formatting with {@link HtmlFormatter}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class DescriptionHtmlTest {
    @Test
    void testDescriptionWithLink() {
        Description description =
                Description.builder()
                        .text("This is a text with a link %s", link("https://somepage", "to here"))
                        .build();

        String formattedDescription = new HtmlFormatter().format(description);

        assertThat(formattedDescription)
                .isEqualTo(
                        "This is a text with a link <a href=\"https://somepage\">" + "to here</a>");
    }

    @Test
    void testDescriptionWithPercents() {
        Description description =
                Description.builder()
                        .text("This is a text that has some percentage value of 20%.")
                        .build();

        String formattedDescription = new HtmlFormatter().format(description);

        assertThat(formattedDescription)
                .isEqualTo("This is a text that has some percentage value of 20%.");
    }

    @Test
    void testDescriptionWithMultipleLinks() {
        Description description =
                Description.builder()
                        .text(
                                "This is a text with a link %s and another %s",
                                link("https://somepage", "to here"), link("https://link"))
                        .build();

        String formattedDescription = new HtmlFormatter().format(description);

        assertThat(formattedDescription)
                .isEqualTo(
                        "This is a text with a link <a href=\"https://somepage\">to here</a> and another "
                                + "<a href=\"https://link\">https://link</a>");
    }

    @Test
    void testDescriptionWithList() {
        Description description =
                Description.builder()
                        .text("This is some list: ")
                        .list(
                                link("http://first_link"),
                                text(
                                        "this is second element of list with a %s",
                                        link("https://link")))
                        .build();

        String formattedDescription = new HtmlFormatter().format(description);

        assertThat(formattedDescription)
                .isEqualTo(
                        "This is some list: <ul><li><a href=\"http://first_link\">http://first_link"
                                + "</a></li><li>this is second element of list "
                                + "with a <a href=\"https://link\">https://link</a></li></ul>");
    }

    @Test
    void testDescriptionWithLineBreak() {
        Description description =
                Description.builder()
                        .text("This is first line.")
                        .linebreak()
                        .text("This is second line.")
                        .build();

        String formattedDescription = new HtmlFormatter().format(description);

        assertThat(formattedDescription).isEqualTo("This is first line.<br />This is second line.");
    }

    @Test
    void testDescriptionWithListAndEscaping() {
        Description description =
                Description.builder()
                        .text("This is some list: ")
                        .list(text("this is first element with illegal character '>' and '<'"))
                        .build();

        String formattedDescription = new HtmlFormatter().format(description);

        assertThat(formattedDescription)
                .isEqualTo(
                        "This is some list: <ul><li>this is first element with illegal character '&gt;' and '&lt;'</li></ul>");
    }
}
