/*
 * Copyright (c) 2018 Uber Technologies, Inc.
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions
 * of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

package com.uber.marmaray.common.spark;

import com.google.common.base.Optional;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSparkFactory {

    private Optional<SparkFactory> sparkFactory = Optional.absent();

    @Before
    public void setup() {
        // Since we are caching sparkContext in it.
        this.sparkFactory = Optional.of(new SparkFactory());
    }

    @Test
    public void testSparkSessionAndSparkContext() {
        final SparkArgs sparkArgs = getSampleMarmaraySparkArgs();
        final SparkSession sparkSession = sparkFactory.get()
            .getSparkSession(sparkArgs, false);
        assertExpectationsOnSparkContext(sparkArgs, sparkSession.sparkContext());

        // should re-use existing SparkContext and not fail
        final SparkContext sc2 = sparkFactory.get().getSparkContext(sparkArgs).sc();
        assertExpectationsOnSparkContext(sparkArgs, sc2);
    }

    @After
    public void tearDown() {
        final SparkArgs sparkArgs = getSampleMarmaraySparkArgs();
        // gets existing sc
        this.sparkFactory.get().getSparkContext(sparkArgs).sc().stop();
        this.sparkFactory = Optional.absent();
    }

    private void assertExpectationsOnSparkContext(
        @NonNull final SparkArgs sparkArgs,
        @NonNull final SparkContext sc) {
        final String registeredAvroSchemaStr = sc.conf().getAvroSchema().head()._2();
        final Schema expectedAvroSchema = sparkArgs.getAvroSchemas().get().get(0);
        Assert.assertEquals(expectedAvroSchema.toString(), registeredAvroSchemaStr);
        Assert.assertEquals("foo_bar", sc.appName());
        Assert.assertEquals("512", sc.hadoopConfiguration().get("mapreduce.map.memory.mb"));
    }

    private SparkArgs getSampleMarmaraySparkArgs() {
        final Schema recordSchema = SchemaBuilder.record("fooRecord").fields().name("abc").type()
            .intType().intDefault(0).endRecord();
        final Optional<List<Schema>> schemas = Optional.of(Arrays.asList(recordSchema));

        final Map<String, String> overrideSparkProperties = new HashMap<>();
        overrideSparkProperties.put("spark.master", "local[2]");
        overrideSparkProperties.put("spark.app.name", "foo_bar");

        final Configuration hadoopConfiguration = new Configuration();
        hadoopConfiguration.set("mapreduce.map.memory.mb", "512");

        return new SparkArgs(schemas, Arrays.asList(),
            overrideSparkProperties, hadoopConfiguration);
    }
}
