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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.util.TestLogger;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Properties;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link EntrypointClusterConfigurationParserFactory}. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class EntrypointClusterConfigurationParserFactoryTest extends TestLogger {

    private static final CommandLineParser<EntrypointClusterConfiguration> commandLineParser =
            new CommandLineParser<>(new EntrypointClusterConfigurationParserFactory());

    @Test
    public void testEntrypointClusterConfigurationParsing() throws FlinkParseException {
        final String configDir = "/foo/bar";
        final int restPort = 1234;
        final String key = "key";
        final String value = "value";
        final String arg1 = "arg1";
        final String arg2 = "arg2";
        final String[] args = {
            "--configDir",
            configDir,
            "-D",
            "jobmanager.rpc.address=localhost",
            "-D",
            "rest.port=" + restPort,
            String.format("-D%s=%s", key, value),
            arg1,
            arg2
        };

        final EntrypointClusterConfiguration clusterConfiguration = commandLineParser.parse(args);
        final Configuration dynamicPropertiesConfig =
                ConfigurationUtils.createConfiguration(clusterConfiguration.getDynamicProperties());

        assertThat(clusterConfiguration.getConfigDir(), is(equalTo(configDir)));
        assertThat(dynamicPropertiesConfig.get(RestOptions.PORT), is(equalTo(restPort)));
        final Properties dynamicProperties = clusterConfiguration.getDynamicProperties();

        assertThat(dynamicProperties, hasEntry(key, value));

        assertThat(clusterConfiguration.getArgs(), arrayContaining(arg1, arg2));
    }

    @Test
    public void testOnlyRequiredArguments() throws FlinkParseException {
        final String configDir = "/foo/bar";
        final String[] args = {"--configDir", configDir};

        final EntrypointClusterConfiguration clusterConfiguration = commandLineParser.parse(args);
        final Configuration dynamicProperties =
                ConfigurationUtils.createConfiguration(clusterConfiguration.getDynamicProperties());

        assertThat(clusterConfiguration.getConfigDir(), is(equalTo(configDir)));
        assertThat(dynamicProperties.get(RestOptions.PORT), is(equalTo(8081)));
    }

    @Test(expected = FlinkParseException.class)
    public void testMissingRequiredArgument() throws FlinkParseException {
        final String[] args = {};

        commandLineParser.parse(args);
    }
}
