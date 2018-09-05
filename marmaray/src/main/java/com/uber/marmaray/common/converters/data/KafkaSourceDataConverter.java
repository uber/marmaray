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

import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.converters.converterresult.ConverterResult;
import com.uber.marmaray.common.schema.ISchemaService.ISchemaServiceReader;
import com.uber.marmaray.utilities.ErrorExtractor;
import lombok.Getter;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.Function;

import java.util.Collections;
import java.util.List;

/**
 * {@link KafkaSourceDataConverter} extends {@link SourceDataConverter}
 *
 * This class converts kafka messages to {@link AvroPayload}. It uses {@link ISchemaServiceReader} for decoding kafka
 * message bytes.
 */
public class KafkaSourceDataConverter extends SourceDataConverter<Schema, byte[]> {
    @NonNull
    @Getter
    private final ISchemaServiceReader schemaServiceReader;
    @NonNull
    @Getter
    private final List<String> fieldsToCache;

    /**
     *  List of {@Link Function<GenericRecord, GenericRecord>} to apply to the record between reading from kafka and
     *  transferring to the ISource
     */
    @NonNull
    @Getter
    private final List<Function<GenericRecord, GenericRecord>> updateFunctions;

    public KafkaSourceDataConverter(@NonNull final ISchemaServiceReader schemaServiceReader,
            @NonNull final Configuration conf,  @NonNull final ErrorExtractor errorExtractor) {
        this(schemaServiceReader, conf, Collections.emptyList(), Collections.emptyList(), errorExtractor);
    }

    public KafkaSourceDataConverter(@NonNull final ISchemaServiceReader schemaServiceReader,
                                    @NonNull final Configuration conf,
                                    @NonNull final List<Function<GenericRecord, GenericRecord>> updateFunctions,
                                    @NonNull final List<String> fieldsToCache,
                                    @NonNull final ErrorExtractor errorExtractor) {
        super(conf, errorExtractor);
        this.schemaServiceReader = schemaServiceReader;
        this.updateFunctions = updateFunctions;
        this.fieldsToCache = fieldsToCache;
    }

    @Override
    public List<ConverterResult<byte[], AvroPayload>> convert(@NonNull final byte[] data) throws Exception {
        GenericRecord genericRecord = this.schemaServiceReader.read(data);
        for (Function<GenericRecord, GenericRecord> func : this.updateFunctions) {
            genericRecord = func.call(genericRecord);
        }
        return Collections.singletonList(new ConverterResult(new AvroPayload(genericRecord, this.fieldsToCache)));
    }
}
