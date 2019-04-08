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
import com.uber.marmaray.utilities.ErrorExtractor;
import com.uber.marmaray.utilities.GenericRecordUtil;
import com.uber.marmaray.utilities.SchemaUtil;
import com.uber.marmaray.utilities.StringTypes;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.binary.Hex;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

/**
 * {@link FileSinkDataConverter} extends {@link SinkDataConverter}
 * This class converts data from intermediate Avro schema to string with csv format.
 *  This class is only to be used where the sink of the data migration is FileSink.
 *  The main convertAll method of this class will return a RDD of String with csv format to caller.
 *  The getHeader method will return a String of column header for the csv file.
 */
@Slf4j
public abstract class FileSinkDataConverter extends SinkDataConverter<Schema, String> {
    public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS Z";

    public FileSinkDataConverter(@NonNull final Configuration conf, @NonNull final ErrorExtractor errorExtractor) {
        super(conf, errorExtractor);
    }

    public abstract JavaPairRDD<String, String> convertAll(@NonNull final JavaRDD<AvroPayload> data);
    public abstract String getHeader(@NonNull final JavaRDD<AvroPayload> data);

    protected String getValuesString(@NonNull final GenericRecord record, @NonNull final Schema.Field field) {
        final Object rawData = record.get(field.name());
        final Schema schema = field.schema();
        final Schema nonNullSchema = GenericRecordUtil.isOptional(schema) ? GenericRecordUtil.getNonNull(schema)
            : schema;
        if (rawData == null) {
            return StringTypes.EMPTY;
        } else if (SchemaUtil.isTimestampSchema(schema)) {
            final Timestamp ts = SchemaUtil.decodeTimestamp(record.get(field.name()));
            return new SimpleDateFormat(DATE_FORMAT).format(ts);
        } else if (Schema.Type.BYTES.equals(nonNullSchema.getType())) {
            final byte[] bytes = ((ByteBuffer) record.get(field.name())).array();
            return String.format("0x%s", Hex.encodeHexString(bytes));
        } else {
            return record.get(field.name()).toString();
        }
    }
}
