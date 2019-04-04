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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.json.JSONObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import scala.Tuple2;

/**
 * {@link FileSinkDataJSONConverter} extends {@link SinkDataConverter}
 */
@Slf4j
public class FileSinkDataJSONConverter extends FileSinkDataConverter {
    public final String fileType;
    public final String row_identifier;

    public FileSinkDataJSONConverter(@NonNull final Configuration conf, @NonNull final ErrorExtractor errorExtractor) {
        super(conf, errorExtractor);
        final FileSinkConfiguration fsConf = new FileSinkConfiguration(conf);
        this.fileType = fsConf.getFileType();
        this.row_identifier = fsConf.getRowIdentifier();
    }

    @Override
    public void setJobMetrics(final JobMetrics jobMetrics) {
        // ignored
    }

    /**
     * This API generates RDDPair for saving different file format(sequence, json,...)
     * @param data
     * @return
     */
    @Override
    public JavaPairRDD<String, String> convertAll(@NonNull final JavaRDD<AvroPayload> data) {
        try {
            Preconditions.checkNotNull(data.first());
        } catch (Exception e) {
            if (this.topicMetrics.isPresent()) {
                this.topicMetrics.get().createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_ERROR, 1,
                        DataFeedMetricNames.getErrorModuleCauseTags(
                                ModuleTagNames.SINK_CONVERTER, ErrorCauseTagNames.NO_DATA));
            }
            log.error("No data to convert");
            throw e;
        }
        final String key = data.first().getData().getSchema().getName();
        if (this.row_identifier.isEmpty()) {
            log.warn("row identifier is missing, schema name {} will be is used", key);
        } else {
            final Object field = data.first().getField(this.row_identifier);
            if (field == null) {
                final String errorMessage = String.format(
                        "specified row identifier field : {} does not exist", row_identifier);
                if (this.topicMetrics.isPresent()) {
                    this.topicMetrics.get().createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_ERROR, 1,
                            DataFeedMetricNames.getErrorModuleCauseTags(
                                    ModuleTagNames.SINK_CONVERTER, ErrorCauseTagNames.EMPTY_FIELD));

                }
                throw new UnsupportedOperationException(errorMessage);
            } else {
                log.info("schema field {} is used for row identifier.", this.row_identifier);
            }
        }
        final JavaPairRDD<String, String> lines = data.mapToPair(row -> {
                final String line = convert(row).get(0).getSuccessData().get().getData();
                final String rowKey = this.row_identifier.isEmpty() ? key
                        : row.getData().get(this.row_identifier).toString();
                return new Tuple2<>(rowKey, line);
            });
        return lines;
    }

    @Override
    public List<ConverterResult<AvroPayload, String>> convert(@NonNull final AvroPayload data)
            throws UnsupportedOperationException {
        final Map<String, Object> jsonMap = getJSONMap(data);
        final JSONObject jsonObject = new JSONObject(jsonMap);
        return Collections.singletonList(new ConverterResult<>(jsonObject.toString()));
    }

    private Map<String, Object> getJSONMap(@NonNull final AvroPayload data) {
        Map<String, Object> jsonMap = new HashMap<>();
        final GenericRecord record = data.getData();
        final List<Schema.Field> fields = record.getSchema().getFields();
        fields.forEach(field -> jsonMap.put(field.name(), getValuesString(record, field)));
        return jsonMap;
    }

    /**
     * No Column Header for json format
     * @param data
     * @return
     */
    @Override
    public String getHeader(@NonNull final JavaRDD<AvroPayload> data) {
        return null;
    }
}
