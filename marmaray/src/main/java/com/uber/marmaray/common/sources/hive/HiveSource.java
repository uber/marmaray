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
package com.uber.marmaray.common.sources.hive;

import com.google.common.base.Optional;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.HiveSourceConfiguration;
import com.uber.marmaray.common.converters.data.SparkSourceDataConverter;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.metrics.DataFeedMetricNames;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.ErrorCauseTagNames;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.common.metrics.ModuleTagNames;
import com.uber.marmaray.common.sources.ISource;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import parquet.Preconditions;

import java.io.Serializable;
import java.util.List;

@Slf4j
@AllArgsConstructor
public class HiveSource implements ISource<ParquetWorkUnitCalculatorResult, HiveRunState>, Serializable {

    @Getter
    final HiveSourceConfiguration hiveConf;

    private final SQLContext sqlContext;

    private final SparkSourceDataConverter converter;

    @Getter
    private long inputCount;

    @Getter
    private Optional<DataFeedMetrics> dataFeedMetrics = Optional.absent();

    public HiveSource(@NonNull final HiveSourceConfiguration hiveConf,
                      @NonNull final SQLContext sqlContext,
                      @NonNull final SparkSourceDataConverter converter) {
        this.hiveConf = hiveConf;
        this.sqlContext = sqlContext;
        this.converter = converter;
        this.inputCount = 0;
    }

    @Override
    public void setDataFeedMetrics(final DataFeedMetrics dataFeedMetrics) {
        this.converter.setDataFeedMetrics(dataFeedMetrics);
        this.dataFeedMetrics = Optional.of(dataFeedMetrics);
    }

    @Override
    public void setJobMetrics(final JobMetrics jobMetrics) {
        // ignored
    }

    @Override
    public JavaRDD<AvroPayload> getData(@NonNull final ParquetWorkUnitCalculatorResult workUnitCalcResult) {
        Preconditions.checkState(workUnitCalcResult.hasWorkUnits(),
                "No work to process for: " + hiveConf.getDataPath());

        /**
         * Current implementation of HiveSource assumes that only a single work unit exists which
         * corresponds to the single partition that is processed per job.
         */
        final List<String> workUnits = workUnitCalcResult.getWorkUnits();

        final String hdfsPath = new Path(this.hiveConf.getDataPath(), workUnits.get(0)).toString();

        log.info("Reading data from path: {}", hdfsPath);
        final Dataset<Row> data;
        try {
            data = this.sqlContext.read().parquet(hdfsPath);
            this.inputCount = data.count();
        } catch (Exception e) {
            if (this.dataFeedMetrics.isPresent()) {
                this.dataFeedMetrics.get().createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_ERROR, 1,
                        DataFeedMetricNames.getErrorModuleCauseTags(
                                ModuleTagNames.SOURCE, ErrorCauseTagNames.FILE_PATH));
            }
            log.error(String.format("Error reading from source path"), e);
            throw new JobRuntimeException(e);
        }

        final int numPartitions = calculateHiveNumPartitions(data);
        log.info("Using {} partitions", numPartitions);

        return this.converter.map(data
                .coalesce(numPartitions)
                .javaRDD()).getData();
    }

    private int calculateHiveNumPartitions(@NonNull final Dataset<Row> data) {
        /*
         * For now we just return the number of partitions in the underlying RDD, but in the future we can define
         * the type of strategy in the configuration and heuristically calculate the number of partitions.
         *
         * todo: T923425 to actually do the heuristic calculation to optimize num partitions
         */
        return data.rdd().getNumPartitions();
    }
}
