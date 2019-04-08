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

package com.uber.marmaray.common.sources.file;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.FileSourceConfiguration;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.common.sources.ISource;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import java.io.IOException;
import java.util.stream.Collectors;

@AllArgsConstructor
public class FileSource implements ISource<FileWorkUnitCalculator.FileWorkUnitCalculatorResult, FileRunState> {

    private final FileSourceConfiguration conf;
    private final Optional<JavaSparkContext> jsc;
    private final FileSourceDataConverter converter;

    @Override
    public void setDataFeedMetrics(@NonNull final DataFeedMetrics dataFeedMetrics) {
        // ignoring
    }

    @Override
    public void setJobMetrics(@NonNull final JobMetrics jobMetrics) {
        // ignoring
    }

    @Override
    public JavaRDD<AvroPayload> getData(@NonNull final FileWorkUnitCalculator.FileWorkUnitCalculatorResult result) {
        Preconditions.checkState(result.hasWorkUnits(), "no work to do: " + this.conf.getDirectory());
        // todo: support more types
        Preconditions.checkState(this.conf.getType().equals("json"), "only json files supported");
        try {
            final FileSystem fs = this.conf.getFileSystem();
            final String filesToRead = result.getWorkUnits().stream()
                .map(LocatedFileStatus::getPath)
                .map(Path::toString)
                .collect(Collectors.joining(","));
            final RDD<String> fileRows = this.jsc.get().sc().textFile(filesToRead, 1);
            return this.converter.map(fileRows.toJavaRDD()).getData();

        } catch (IOException e) {
            throw new JobRuntimeException("Error getting files", e);
        }
    }
}
