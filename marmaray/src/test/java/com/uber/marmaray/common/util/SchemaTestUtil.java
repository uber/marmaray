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
package com.uber.marmaray.common.util;

import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.uber.marmaray.common.schema.cassandra.CassandraSchemaField;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * Helper class to generate test data and {@link Schema}.
 */
public final class SchemaTestUtil {
    private static final Random r = new Random();

    private SchemaTestUtil() {
        throw new JobRuntimeException("helper class; don't instantiate it");
    }

    public static Schema getSchema(@NotEmpty final String tsKey, @NotEmpty final String recordKey,
        final int mandatoryFields, final int optionalFields) {
        final RecordBuilder<Schema> recordSchema = SchemaBuilder.builder().record("test");
        final FieldAssembler<Schema> fields = recordSchema.fields();
        fields.requiredDouble(tsKey);
        fields.requiredString(recordKey);
        for (int i = 0; i < mandatoryFields; i++) {
            fields.requiredString("testMandatory" + i);
        }
        for (int i = 0; i < optionalFields; i++) {
            fields.optionalString("testOptional" + i);
        }
        return fields.endRecord();
    }

    public static List<AvroPayload> getRandomData(@NotEmpty final String schemaStr,
        @NotEmpty final String tsKey, @NotEmpty final String recordKey, final int count) {

        final Schema schema = (new Schema.Parser()).parse(schemaStr);
        final List<AvroPayload> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final GenericRecord record = new GenericData.Record(schema);
            for (Field field : schema.getFields()) {
                record.put(field.name(), new String("value" + System.nanoTime()));
            }
            record.put(tsKey, new Double(System.currentTimeMillis()));
            record.put(recordKey, "key" + r.nextLong());
            records.add(new AvroPayload(record));
        }
        return records;
    }

    public static Schema getSchema(@NotEmpty final String type) {

        final SchemaBuilder.TypeBuilder<Schema> schema = SchemaBuilder.builder();

        switch (type) {
            case CassandraSchemaField.STRING_TYPE:
                return schema.stringType();
            case CassandraSchemaField.BOOLEAN_TYPE:
                return schema.booleanType();
            case CassandraSchemaField.DOUBLE_TYPE:
                return schema.doubleType();
            case CassandraSchemaField.FLOAT_TYPE:
                return schema.floatType();
            case CassandraSchemaField.LONG_TYPE:
                return schema.longType();
            case CassandraSchemaField.INT_TYPE:
                return schema.intType();
            default:
                throw new IllegalArgumentException("Type " + type + " invalid for converting to Cassandra field");

        }
    }

}
