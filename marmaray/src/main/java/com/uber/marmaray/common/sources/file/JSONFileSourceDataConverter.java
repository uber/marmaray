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

import com.uber.hoodie.avro.MercifulJsonConverter;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.converters.converterresult.ConverterResult;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.utilities.ErrorExtractor;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.Collections;
import java.util.List;

public class JSONFileSourceDataConverter extends FileSourceDataConverter {

    private final String schemaString;
    private transient Schema schema;

    public JSONFileSourceDataConverter(@NonNull final Configuration conf, @NonNull final ErrorExtractor errorExtractor,
                                       @NonNull final Schema schema) {
        super(conf, errorExtractor);
        this.schema = schema;
        this.schemaString = schema.toString();
    }

    public Schema getSchema() {
        if (this.schema == null) {
            this.schema = new Schema.Parser().parse(this.schemaString);
        }
        return this.schema;
    }

    @Override
    public void setDataFeedMetrics(final DataFeedMetrics dataFeedMetrics) {
        //ignored
    }

    @Override
    public void setJobMetrics(final JobMetrics jobMetrics) {
        // ignored
    }

    @Override
    protected List<ConverterResult<String, AvroPayload>> convert(@NonNull final String data) throws Exception {
        try {
            final GenericRecord gr = new MercifulJsonConverter(getSchema()).convert(data);
            return Collections.singletonList(
                new ConverterResult<String, AvroPayload>(new AvroPayload(gr)));
        } catch (MercifulJsonConverter.JsonConversionException e) {
            return Collections.singletonList(new ConverterResult<String, AvroPayload>(data, e.getMessage()));
        }
    }
}
