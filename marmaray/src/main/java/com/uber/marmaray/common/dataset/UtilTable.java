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
package com.uber.marmaray.common.dataset;

import com.google.common.annotations.VisibleForTesting;
import com.uber.marmaray.utilities.SparkUtil;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static com.uber.marmaray.utilities.DateUtil.DATE_PARTITION_FORMAT;

/**
 * {@link UtilTable} is represented as a {@link Dataset} of {@link UtilRecord},
 * which is converted from a {@link JavaRDD} of {@link UtilRecord}. We can extend
 * the functionality of {@link UtilTable} same as that of {@link Dataset}
 */
@Slf4j
@AllArgsConstructor
public class UtilTable<T extends UtilRecord> {
    private final SparkSession spark;
    private final Dataset<T> dataset;
    private final Path destPath;
    private final boolean isDatePartitioned;

    public UtilTable(@NonNull final Class<T> type,
                     @NonNull final JavaRDD<T> javaRDD,
                     @NonNull final Path destPath,
                     final boolean isDatePartitioned) {
        this(type, javaRDD, destPath, isDatePartitioned, SparkUtil.getOrCreateSparkSession());
    }

    public UtilTable(@NonNull final Class<T> type,
        @NonNull final JavaRDD<T> javaRDD,
        @NonNull final Path destPath,
        final boolean isDatePartitioned,
        @NonNull final SparkSession sparkSession) {
        this.spark = sparkSession;
        final RDD<T> rdd = javaRDD.rdd();
        final Encoder<T> bean = Encoders.bean(type);
        this.dataset = this.spark.createDataset(rdd, bean);
        this.destPath = destPath;
        this.isDatePartitioned = isDatePartitioned;
    }

    public void writeParquet() throws IOException {
        // TODO: Consider having a configuration to limit number records written out
        this.dataset.write().mode(SaveMode.Append).parquet(getDestWritePath().toString());
    }

    public Long size() {
        return this.dataset.count();
    }

    @VisibleForTesting
    public void show() {
        this.dataset.show();
    }

    public Path getDestWritePath() {
        return this.isDatePartitioned ? getDestDatePartitionedPath() : this.destPath;
    }

    private Path getDestDatePartitionedPath() {
        final ZonedDateTime date = ZonedDateTime.now(ZoneOffset.UTC);
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_PARTITION_FORMAT);
        return new Path(this.destPath, date.format(formatter));
    }
}
