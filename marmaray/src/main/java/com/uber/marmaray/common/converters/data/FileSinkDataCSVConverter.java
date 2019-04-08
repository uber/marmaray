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

package com.uber.marmaray.common.converters.data;

import com.google.common.base.Preconditions;
import com.opencsv.CSVWriter;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.FileSinkConfiguration;
import com.uber.marmaray.common.converters.converterresult.ConverterResult;
import com.uber.marmaray.common.metrics.DataFeedMetricNames;
import com.uber.marmaray.common.metrics.ErrorCauseTagNames;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.common.metrics.ModuleTagNames;
import com.uber.marmaray.utilities.ErrorExtractor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.io.StringWriter;
import java.util.Collections;
import java.util.List;

/**
 * {@link FileSinkDataCSVConverter} extends {@link FileSinkDataConverter}
 *  This class converts data from intermediate Avro schema to string with csv format.
 *  This class is only to be used where the sink of the data migration is FileSink.
 *  The main convertAll method of this class will return a RDD of String with csv format to caller.
 *  The getHeader method will return a String of column header for the csv file.
 */

@Slf4j
public class FileSinkDataCSVConverter extends FileSinkDataConverter {
    public static final String CSV = "csv";
    public final char separator;
    public final String fileType;

    public FileSinkDataCSVConverter(@NonNull final Configuration conf,
                                    @NonNull final ErrorExtractor errorExtractor) {
        super(conf, errorExtractor);
        final FileSinkConfiguration fsConf = new FileSinkConfiguration(conf);
        this.fileType = fsConf.getFileType();
        this.separator = fsConf.getSeparator();
    }

    @Override
    public void setJobMetrics(final JobMetrics jobMetrics) {
        // ignored
    }

    /**
     * This method converts RDD of AvroPayload data to RDD of String with specified file type.
     * Currently supports csv file only.
     * @param data
     * @return
     * @throws UnsupportedOperationException
     */
    @Override
    public JavaPairRDD<String, String> convertAll(@NonNull final JavaRDD<AvroPayload> data) {
        try {
            Preconditions.checkArgument(!data.isEmpty());
        } catch (Exception e) {
            if (this.topicMetrics.isPresent()) {
                this.topicMetrics.get().createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_ERROR, 1,
                        DataFeedMetricNames.getErrorModuleCauseTags(
                                ModuleTagNames.SINK_CONVERTER, ErrorCauseTagNames.NO_DATA));
            }
            log.error("No data to convert");
            throw e;
        }
        final String rowKey = data.first().getData().getSchema().getName();
        final JavaPairRDD<String, String> lines = data.mapToPair(row -> {
                final String line = this.convert(row).get(0).getSuccessData().get().getData();
                log.debug("Line: {}", line);
                return new Tuple2<>(rowKey, line);
            });
        return lines;
    }

    @Override
    public List<ConverterResult<AvroPayload, String>> convert(@NonNull final AvroPayload data)
            throws UnsupportedOperationException {
        final String line;
        if (this.fileType.equals(CSV)) {
            final GenericRecord r = data.getData();
            final String[] tmp = r.getSchema().getFields().stream().map(f -> getValuesString(r, f))
                    .toArray(String[]::new);
            final StringWriter sw = new StringWriter();
            final CSVWriter writer = new CSVWriter(sw
                    , this.separator, '\"', '\\', "");
            writer.writeNext(tmp, false);
            line = sw.toString();
        } else {
            final String errorMessage = String.format("Format %s not supported yet.", this.fileType);
            throw new UnsupportedOperationException(errorMessage);
        }
        return Collections.singletonList(new ConverterResult<>(line));
    }

    /**
     * This methods get the column header of data.
     * It specifically works for file type: csv.
     * @param data
     * @return String of column header separated by separator.
     */
    @Override
    public String getHeader(@NonNull final JavaRDD<AvroPayload> data) {
        final AvroPayload line = data.first();
        final String[] headList
                = line.getData().getSchema().getFields().stream().map(f -> f.name()).toArray(String[]::new);
        final StringWriter sw = new StringWriter();
        final CSVWriter writer = new CSVWriter(sw
                , this.separator, '\"', '\\', "");
        writer.writeNext(headList, false);
        return sw.toString();
    }
}
