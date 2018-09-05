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

import com.google.common.collect.ImmutableMap;
import com.uber.marmaray.common.util.AbstractSparkTest;
import com.uber.marmaray.common.util.FileTestUtil;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.hibernate.validator.constraints.NotEmpty;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link UtilTable} creation
 */
@Slf4j
public class TestUtilTable extends AbstractSparkTest {

    private static final Long JOB_START_TIME = Instant.now().getEpochSecond();
    private static final String TABLE_NAME = "test-table";

    @Test
    public void testWriteErrorTable() throws IOException {
        final String dest = "errors";
        testWriteUtilTable(ErrorRecord.class, generateTestErrorRecords(), dest, true);
        testWriteUtilTable(ErrorRecord.class, generateTestErrorRecords(), dest, false);
    }

    @Test
    public void testWriteExceptionTable() throws IOException {
        final String destFolder = "exceptions";
        testWriteUtilTable(ExceptionRecord.class, generateTestExceptionRecords(), destFolder, true);
        testWriteUtilTable(ExceptionRecord.class, generateTestExceptionRecords(), destFolder, false);
    }

    @Test
    public void testWriteMetricTable() throws IOException {
        final String destFolder = "metrics";
        testWriteUtilTable(MetricRecord.class, generateTestMetricRecords(), destFolder, true);
        testWriteUtilTable(MetricRecord.class, generateTestMetricRecords(), destFolder, false);
    }

    public void testWriteUtilTable(@NonNull final Class type,
                                   @NonNull final JavaRDD utilRecords,
                                   @NotEmpty final String destFolder,
                                   final boolean isDatePartitioned) throws IOException {
        final String basePath = FileTestUtil.getTempFolder();
        final Path destPath = new Path(basePath, destFolder);
        final UtilTable utilTable = new UtilTable(type, utilRecords, destPath, isDatePartitioned, spark.get());
        final Path destWritePath = utilTable.getDestWritePath();

        assertEquals(new Long(10), utilTable.size());
        assertFalse(this.fileSystem.get().exists(destPath));

        utilTable.show();
        utilTable.writeParquet();
        final FileStatus[] destPathChildren = this.fileSystem.get().listStatus(destWritePath);

        log.debug("Destination folder content:");
        Stream.of(destPathChildren).forEach(f -> log.debug(f.getPath().toString()));

        assertTrue(this.fileSystem.get().exists(destWritePath));
        assertTrue(destPathChildren.length > 0);

        final Dataset outputDataset = spark.get().read().parquet(destWritePath.toString());
        log.debug("Output dataset content");
        outputDataset.show();

        final List<String> datasetFieldNames = Arrays.asList(outputDataset.schema().fieldNames());
        final List<String> requiredFieldNames = Arrays.asList("application_id", "job_name", "job_start_timestamp", "timestamp");
        assertTrue(datasetFieldNames.containsAll(requiredFieldNames));
    }

    private JavaRDD<ErrorRecord> generateTestErrorRecords() {
        final List<ErrorRecord> recordList = new ArrayList<>();
        IntStream.range(0, 10).forEach(i -> {
            final Long timestamp = Instant.now().getEpochSecond();
            final String rowKey = "row-key-abc-" + i;
            final String columnName = "column" + i;
            final String content = "This is content for " + rowKey;
            final String errorType = "TestErrorType";
            final ErrorRecord errorRecord = new ErrorRecord(
                    getAppId(), JOB_NAME, JOB_START_TIME, timestamp,
                    TABLE_NAME, rowKey, columnName, content, errorType);
            recordList.add(errorRecord);
        });
        return jsc.get().parallelize(recordList);
    }

    private JavaRDD<ExceptionRecord> generateTestExceptionRecords() {
        final List<ExceptionRecord> recordList = new ArrayList<>();
        IntStream.range(0, 10).forEach(i -> {
            final Long timestamp = Instant.now().getEpochSecond();
            final String exception = "RuntimeException " + i;
            final String exceptionMessage = "A RuntimeException message " + i;
            final String stacktrace = "Very long stacktrace " + i;
            final boolean is_driver = false;
            final ExceptionRecord exceptionRecord = new ExceptionRecord(
                    getAppId(), JOB_NAME, JOB_START_TIME, timestamp,
                    exception, exceptionMessage, stacktrace, is_driver);
            recordList.add(exceptionRecord);
        });
        return jsc.get().parallelize(recordList);
    }

    private JavaRDD<MetricRecord> generateTestMetricRecords() {
        final List<MetricRecord> recordList = new ArrayList<>();
        IntStream.range(0, 10).forEach(i -> {
            final Long timestamp = Instant.now().getEpochSecond();
            final String metricName = "MetricName " + i;
            final long metricValue = i;
            final String tags = ImmutableMap.of("tag1-" + i, "tagVal1-" + i, "tag2-" + i, "tagVal2-" + i).toString();
            final MetricRecord metricRecord = new MetricRecord(
                    getAppId(), JOB_NAME, JOB_START_TIME, timestamp,
                    metricName, metricValue, tags);
            recordList.add(metricRecord);
        });
        return jsc.get().parallelize(recordList);
    }
}
