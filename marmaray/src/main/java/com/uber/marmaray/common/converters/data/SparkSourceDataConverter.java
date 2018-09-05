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

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.converters.converterresult.ConverterResult;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.utilities.ErrorExtractor;
import com.uber.marmaray.utilities.SparkUtil;
import com.uber.marmaray.utilities.StringTypes;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link SparkSourceDataConverter} extends {@link SourceDataConverter}
 *
 * This converts data from Spark format to the common intermediate {@link AvroPayload} format
 */
@Slf4j
public class SparkSourceDataConverter extends SourceDataConverter<StructType, Row> {
    private static final Set<DataType> supportedDataTypes = SparkUtil.getSupportedDataTypes();

    private final String jsonOutputSchema;
    private final StructField[] fields;
    private final Set<String> requiredKeys;
    private Optional<Schema> outputSchema = Optional.absent();

    public SparkSourceDataConverter(@NonNull final StructType inputSchema,
                                    @NonNull final Schema outputSchema,
                                    @NonNull final Configuration conf,
                                    @NonNull final Set<String> requiredKeys,
                                    @NonNull final ErrorExtractor errorExtractor) {
        super(conf, errorExtractor);
        this.fields = Arrays.stream(inputSchema.fields())
                .filter(f -> !f.name().startsWith("_")).toArray(StructField[]::new);
        this.jsonOutputSchema = outputSchema.toString();
        this.requiredKeys = requiredKeys;
    }

    @Override
    public List<ConverterResult<Row, AvroPayload>> convert(@NonNull final Row row) throws Exception {
        if (!this.outputSchema.isPresent()) {
            this.outputSchema = Optional.of((new Schema.Parser().parse(this.jsonOutputSchema)));
        }

        Preconditions.checkNotNull(row.schema());

        final GenericRecord gr = new GenericData.Record(this.outputSchema.get());

        final Set<String> required = this.requiredKeys.stream().map(String::new).collect(Collectors.toSet());

        // todo: think about generalizing this, the pattern is the same
        for (int i = 0; i < this.fields.length; i++) {
            required.remove(this.fields[i].name());
            final DataType dt = this.fields[i].dataType();

            try {
                final Object data = row.getAs(this.fields[i].name());
                if (supportedDataTypes.contains(dt)) {
                    gr.put(this.fields[i].name(), data);
                } else {
                    throw new JobRuntimeException(dt.toString() + " field type is not supported at this time");
                }
            } catch (final IllegalArgumentException e) {
                // the fieldname did not exist in the row which is ok, skip it
                continue;
            }
        }

        if (!required.isEmpty()) {
            final Joiner joiner = Joiner.on(StringTypes.COMMA);
            final String errMsg = String.format("Required fields were missing. Fields: {}", joiner.join(required));
            throw new JobRuntimeException(errMsg);
        }

        return Collections.singletonList(new ConverterResult<>(new AvroPayload(gr)));
    }
}
