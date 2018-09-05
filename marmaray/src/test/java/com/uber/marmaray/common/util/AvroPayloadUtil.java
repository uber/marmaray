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

import com.google.common.base.Strings;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.utilities.StringTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public final class AvroPayloadUtil {

    private AvroPayloadUtil() {
        throw new JobRuntimeException("Utility class should not be instantiated");
    }

    public static JavaRDD<AvroPayload> generateTestData(final JavaSparkContext jsc,
                                                        final int numRecords,
                                                        final String fieldToExclude) {
        final List<AvroPayload> payloads = new ArrayList<>();

        final Schema schema = getAvroTestDataSchema(fieldToExclude);

        for (int i = 1; i <= numRecords; i++) {

            final GenericRecord gr = new GenericData.Record(schema);

            if (!CassandraTestConstants.INT_FIELD.equals(fieldToExclude)) {
                gr.put(CassandraTestConstants.INT_FIELD, i);
            }

            if (!CassandraTestConstants.STRING_FIELD.equals(fieldToExclude)) {
                gr.put(CassandraTestConstants.STRING_FIELD, Integer.toString(i));
            }

            if (!CassandraTestConstants.BOOLEAN_FIELD.equals(fieldToExclude)) {
                gr.put(CassandraTestConstants.BOOLEAN_FIELD, true);
            }

            final AvroPayload ap = new AvroPayload(gr);
            payloads.add(ap);
        }

        return jsc.parallelize(payloads);
    }

    public static JavaRDD<AvroPayload> generateTestDataNew(final JavaSparkContext jsc,
                                                        final int numRecords,
                                                        final String fieldToExclude) {
        final List<AvroPayload> payloads = new ArrayList<>();

        final Schema schema = getAvroTestDataSchema(fieldToExclude);

        for (int i = 1; i <= numRecords; i++) {

            final GenericRecord gr = new GenericData.Record(schema);

            if (!CassandraTestConstants.INT_FIELD.equals(fieldToExclude)) {
                gr.put(CassandraTestConstants.INT_FIELD, i);
            }

            if (!CassandraTestConstants.STRING_FIELD.equals(fieldToExclude)) {
                gr.put(CassandraTestConstants.STRING_FIELD, Integer.toString(i)+"\""+",try\\");
            }

            if (!CassandraTestConstants.BOOLEAN_FIELD.equals(fieldToExclude)) {
                gr.put(CassandraTestConstants.BOOLEAN_FIELD, true);
            }

            final AvroPayload ap = new AvroPayload(gr);
            payloads.add(ap);
        }

        return jsc.parallelize(payloads);
    }

    public static Schema getAvroTestDataSchema(final String fieldToExclude) {

        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("commonSchema").fields();

        if (!CassandraTestConstants.INT_FIELD.equals(fieldToExclude)) {
            fields = fields.name(CassandraTestConstants.INT_FIELD).type().intType().noDefault();
        }

        if (!CassandraTestConstants.STRING_FIELD.equals(fieldToExclude)) {
            fields = fields.name(CassandraTestConstants.STRING_FIELD).type().stringType().noDefault();
        }

        if (!CassandraTestConstants.BOOLEAN_FIELD.equals(fieldToExclude)) {
            fields = fields.name(CassandraTestConstants.BOOLEAN_FIELD).type().booleanType().noDefault();
        }

        return fields.endRecord();
    }

    public static List<String> getSchemaFields() {
        return getSchemaFields(StringTypes.EMPTY);
    }

    public static List<String> getSchemaFields(final String fieldToExclude) {
        List<String> fields = new LinkedList<>(Arrays.asList(
                CassandraTestConstants.INT_FIELD,
                CassandraTestConstants.STRING_FIELD,
                CassandraTestConstants.BOOLEAN_FIELD));

        if (!Strings.isNullOrEmpty(fieldToExclude)) {
               fields.remove(fieldToExclude);
        }

        return Collections.unmodifiableList(fields);
    }
}
