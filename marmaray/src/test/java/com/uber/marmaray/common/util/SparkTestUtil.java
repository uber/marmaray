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
package com.uber.marmaray.common.util;

import lombok.NonNull;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkTestUtil {

    public static JavaSparkContext getSparkContext(@NonNull final SparkConf sparkConf) {
        return new JavaSparkContext(sparkConf);
    }

    public static SparkConf getSparkConf(final String appName) {
        return new SparkConf().setAppName(appName)
                .setMaster("local[4]")
                .set("spark.default.parallelism", "4")
                // set the SPARK_LOCAL_IP address only for unit tests
                .set("spark.driver.bindAddress", "127.0.0.1")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryoserializer.buffer.max", "512m");
    }

    public static SparkSession getSparkSession(@NonNull final SparkConf sparkConf) {
        final SparkContext sc = SparkTestUtil.getSparkContext(sparkConf).sc();

        final SparkSession session = org.apache.spark.sql.SparkSession.builder()
                .master("local[*]")
                .sparkContext(sc)
                .getOrCreate();

        return session;
    }
}
