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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat.Codec;
import org.apache.flink.formats.avro.generated.Colors;
import org.apache.flink.formats.avro.generated.Fixed2;
import org.apache.flink.formats.avro.generated.User;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.legacy.io.TextInputFormat;
import org.apache.flink.test.util.JavaProgramTestBaseJUnit4;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.test.util.TestBaseUtils.asFile;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for the {@link AvroOutputFormat}. */
@SuppressWarnings("serial")
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class AvroOutputFormatITCase extends JavaProgramTestBaseJUnit4 {

    public static String outputPath1;

    public static String outputPath2;

    public static String inputPath;

    public static String userData =
            "alice|1|blue\n" + "bob|2|red\n" + "john|3|yellow\n" + "walt|4|black\n";

    @Override
    protected void preSubmit() throws Exception {
        inputPath = createTempFile("user", userData);
        outputPath1 = getTempDirPath("avro_output1");
        outputPath2 = getTempDirPath("avro_output2");
    }

    @Override
    protected void testProgram() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, Integer, String>> input =
                env.createInput(new TextInputFormat(new Path(inputPath)))
                        .map(
                                x -> {
                                    String[] splits = x.split("\\|");
                                    return Tuple3.of(
                                            splits[0], Integer.valueOf(splits[1]), splits[2]);
                                })
                        .returns(
                                TypeInformation.of(
                                        new TypeHint<Tuple3<String, Integer, String>>() {}));

        // output the data with AvroOutputFormat for specific user type
        DataStream<User> specificUser = input.map(new ConvertToUser());
        AvroOutputFormat<User> avroOutputFormat =
                new AvroOutputFormat<>(new Path(outputPath1), User.class);
        avroOutputFormat.setCodec(Codec.SNAPPY); // FLINK-4771: use a codec
        avroOutputFormat.setSchema(
                User.SCHEMA$); // FLINK-3304: Ensure the OF is properly serializing the schema
        specificUser.addSink(new OutputFormatSinkFunction<>(avroOutputFormat));

        // output the data with AvroOutputFormat for reflect user type
        DataStream<ReflectiveUser> reflectiveUser = specificUser.map(new ConvertToReflective());
        reflectiveUser.addSink(
                new OutputFormatSinkFunction<>(
                        new AvroOutputFormat<>(new Path(outputPath2), ReflectiveUser.class)));

        env.execute();
    }

    @Override
    protected void postSubmit() throws Exception {
        // compare result for specific user type
        File[] output1;
        File file1 = asFile(outputPath1);
        if (file1.isDirectory()) {
            output1 = file1.listFiles();
            // check for avro ext in dir.
            for (File avroOutput : Objects.requireNonNull(output1)) {
                assertThat(avroOutput.toString()).endsWith(".avro");
            }
        } else {
            output1 = new File[] {file1};
        }
        List<String> result1 = new ArrayList<>();
        DatumReader<User> userDatumReader1 = new SpecificDatumReader<>(User.class);
        for (File avroOutput : output1) {

            DataFileReader<User> dataFileReader1 =
                    new DataFileReader<>(avroOutput, userDatumReader1);
            while (dataFileReader1.hasNext()) {
                User user = dataFileReader1.next();
                result1.add(
                        user.getName()
                                + "|"
                                + user.getFavoriteNumber()
                                + "|"
                                + user.getFavoriteColor());
            }
        }
        assertThat(result1).contains(userData.split("\n"));

        // compare result for reflect user type
        File[] output2;
        File file2 = asFile(outputPath2);
        if (file2.isDirectory()) {
            output2 = file2.listFiles();
        } else {
            output2 = new File[] {file2};
        }
        List<String> result2 = new ArrayList<>();
        DatumReader<ReflectiveUser> userDatumReader2 =
                new ReflectDatumReader<>(ReflectiveUser.class);
        for (File avroOutput : Objects.requireNonNull(output2)) {
            DataFileReader<ReflectiveUser> dataFileReader2 =
                    new DataFileReader<>(avroOutput, userDatumReader2);
            while (dataFileReader2.hasNext()) {
                ReflectiveUser user = dataFileReader2.next();
                result2.add(
                        user.getName()
                                + "|"
                                + user.getFavoriteNumber()
                                + "|"
                                + user.getFavoriteColor());
            }
        }
        assertThat(result2).contains(userData.split("\n"));
    }

    private static final class ConvertToUser
            extends RichMapFunction<Tuple3<String, Integer, String>, User> {

        @Override
        public User map(Tuple3<String, Integer, String> value) {
            User user = new User();
            user.setName(value.f0);
            user.setFavoriteNumber(value.f1);
            user.setFavoriteColor(value.f2);
            user.setTypeBoolTest(true);
            user.setTypeArrayString(Collections.emptyList());
            user.setTypeArrayBoolean(Collections.emptyList());
            user.setTypeEnum(Colors.BLUE);
            user.setTypeMap(Collections.emptyMap());
            user.setTypeBytes(ByteBuffer.allocate(10));
            user.setTypeDate(LocalDate.parse("2014-03-01"));
            user.setTypeTimeMillis(LocalTime.parse("12:12:12"));
            user.setTypeTimeMicros(LocalTime.ofSecondOfDay(0).plus(123456L, ChronoUnit.MICROS));
            user.setTypeTimestampMillis(Instant.parse("2014-03-01T12:12:12.321Z"));
            user.setTypeTimestampMicros(Instant.ofEpochSecond(0).plus(123456L, ChronoUnit.MICROS));
            // 20.00
            user.setTypeDecimalBytes(
                    ByteBuffer.wrap(BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()));
            // 20.00
            user.setTypeDecimalFixed(
                    new Fixed2(BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()));
            return user;
        }
    }

    private static final class ConvertToReflective extends RichMapFunction<User, ReflectiveUser> {

        @Override
        public ReflectiveUser map(User value) {
            return new ReflectiveUser(
                    value.getName().toString(),
                    value.getFavoriteNumber(),
                    value.getFavoriteColor().toString());
        }
    }

    private static class ReflectiveUser {
        private String name;
        private int favoriteNumber;
        private String favoriteColor;

        public ReflectiveUser() {}

        public ReflectiveUser(String name, int favoriteNumber, String favoriteColor) {
            this.name = name;
            this.favoriteNumber = favoriteNumber;
            this.favoriteColor = favoriteColor;
        }

        public String getName() {
            return this.name;
        }

        public String getFavoriteColor() {
            return this.favoriteColor;
        }

        public int getFavoriteNumber() {
            return this.favoriteNumber;
        }
    }
}
