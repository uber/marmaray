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
package com.uber.marmaray.common;

import com.google.common.base.Preconditions;
import com.uber.marmaray.common.data.IData;
import com.uber.marmaray.utilities.SparkUtil;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.hibernate.validator.constraints.NotEmpty;
import scala.reflect.ClassManifestFactory;
import scala.reflect.ClassTag;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.avro.Schema.Type.RECORD;

/**
 * This class contains the Avro data as payload with the schema
 */
// TODO (T962137)
@ToString
@Slf4j
public class AvroPayload implements IPayload<GenericRecord>, IData, Serializable {

    private static final ClassTag<GenericRecord> recordClassTag = ClassManifestFactory.fromClass(GenericRecord.class);
    @NonNull
    private final Map<String, Object> rootFields = new HashMap<>();
    @NonNull
    private final byte[] byteRecord;

    public AvroPayload(@NonNull final GenericRecord record) {
        this.byteRecord = SparkUtil.serialize(record, recordClassTag);
        for (final Schema.Field f : record.getSchema().getFields()) {
            if (!RECORD.equals(f.schema().getType())) {
                this.rootFields.put(f.name(), record.get(f.name()));
            }
        }
    }

    public AvroPayload(@NonNull final GenericRecord record,
        @NonNull final List<String> fieldsToCache) {
        this.byteRecord = SparkUtil.serialize(record, recordClassTag);
        for (final String f : fieldsToCache) {
            this.rootFields.put(f, record.get(f));
        }
    }

    /**
     * Avoid calling it to fetch top level record fields.
     */
    public GenericRecord getData() {
        return SparkUtil.deserialize(this.byteRecord, recordClassTag);
    }

    /**
     * It only supports fetching fields at the root level of the record which are of type other than
     * {@link org.apache.avro.generic.GenericData.Record}.
     *
     * @param fieldName name of the field at the root level of the record.
     */
    public Object getField(@NotEmpty final String fieldName) {
        Preconditions.checkState(this.rootFields.containsKey(fieldName),
            "field is not cached at root level :" + fieldName);
        return this.rootFields.get(fieldName);
    }
}
