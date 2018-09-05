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
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.converters.converterresult.ConverterResult;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.exceptions.MissingPropertyException;
import com.uber.marmaray.common.schema.cassandra.CassandraDataField;
import com.uber.marmaray.common.schema.cassandra.CassandraPayload;
import com.uber.marmaray.common.schema.cassandra.CassandraSchema;
import com.uber.marmaray.utilities.ByteBufferUtil;
import com.uber.marmaray.utilities.ErrorExtractor;
import com.uber.marmaray.utilities.GenericRecordUtil;
import com.uber.marmaray.utilities.SchemaUtil;
import com.uber.marmaray.utilities.TimestampInfo;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link CassandraSinkDataConverter} extends {@link SinkDataConverter}
 * This class converts data from intermediate Avro schema to the Cassandra schema.  This
 * class is only to be used where the sink of the data migration is Cassandra.  The main convert method of this
 * class will return a RDD of Cassandra payloads to the caller.  All keys & values will be serialized as
 * ByteBuffers as required by Cassandra.  All strings are encoded using UTF-8.
 */
public class CassandraSinkDataConverter extends SinkDataConverter<CassandraSchema, CassandraPayload> {

    private static final long serialVersionUID = 1L;

    private final String inputSchemaJson;

    /*
     * The fields to convert are defined in the job configuration from the user. This can be all or a subset of fields
     * from the schema.
     */
    private final Optional<Set<String>> fieldsToConvert;

    /*
     * The required fields that must be populated in the schema.  These keys form the primary/partition/clustering
     * keys in the Cassandra schema and are defined in the job configuration.
     */
    private final List<String> requiredFields;

    private Optional<Schema> inputSchema = Optional.absent();

    private final TimestampInfo timestampInfo;

    /**
     * This constructor gives the option to only convert certain fields from the schema
     * @param inputSchema
     * @param conf
     * @param fieldsToConvert
     * @param requiredFields
     */
    public CassandraSinkDataConverter(@NonNull final Schema inputSchema,
                                      @NonNull final Configuration conf,
                                      @NonNull final Optional<Set<String>> fieldsToConvert,
                                      @NonNull final List<String> requiredFields,
                                      @NonNull final TimestampInfo timestampInfo,
                                      @NonNull final ErrorExtractor errorExtractor) {
        super(conf, errorExtractor);
        if (fieldsToConvert.isPresent()) {
            validate(fieldsToConvert.get(), requiredFields);
        }
        this.inputSchemaJson = inputSchema.toString();
        this.requiredFields = Collections.unmodifiableList(requiredFields);
        this.fieldsToConvert = fieldsToConvert;
        this.timestampInfo = timestampInfo;
    }

    @Override
    public List<ConverterResult<AvroPayload, CassandraPayload>> convert(final AvroPayload avroPayload)
            throws Exception {
        final CassandraPayload row = new CassandraPayload();
        final Set<String> requiredKeysToFind =  new HashSet<>(this.requiredFields);
        if (!this.inputSchema.isPresent()) {
            this.inputSchema = Optional.of(new Schema.Parser().parse(inputSchemaJson));
        }

        for (int i = 0; i < this.inputSchema.get().getFields().size(); i++) {
            final Schema.Field field = this.inputSchema.get().getFields().get(i);
            if (!this.fieldsToConvert.isPresent()
                    || this.fieldsToConvert.isPresent()
                    && this.fieldsToConvert.get().contains(field.name().toLowerCase())) {
                final Object rawData = avroPayload.getData().get(field.name());

                final ByteBuffer bb;

                if (rawData != null) {

                    final Schema nonNullSchema = GenericRecordUtil.isOptional(field.schema())
                            ? GenericRecordUtil.getNonNull(field.schema())
                            : field.schema();
                    final Schema.Type type = nonNullSchema.getType();

                    switch (type) {
                        case BOOLEAN:
                            final Boolean boolData = (Boolean) rawData;
                            bb = BooleanType.instance.decompose(boolData);
                            break;
                        case INT:
                            final Integer intData = (Integer) rawData;
                            bb = Int32Type.instance.decompose(intData);
                            break;
                        case LONG:
                            final Long longData = (Long) rawData;
                            bb = LongType.instance.decompose(longData);
                            break;
                        case DOUBLE:
                            final Double doubleData = (Double) rawData;
                            bb = DoubleType.instance.decompose(doubleData);
                            break;
                        case STRING:
                            final String strData = rawData.toString();
                            bb = ByteBufferUtil.wrap(strData);
                            break;
                        case FLOAT:
                            final Float floatData = (Float) rawData;
                            bb = FloatType.instance.decompose(floatData);
                            break;
                        // todo(T936057) - add support for non-primitive types
                        default:
                            throw new JobRuntimeException("Type " + field.schema().getType() + " not supported");
                    }
                } else {
                    if (requiredKeysToFind.contains(field.name())) {
                        throw new JobRuntimeException("Data for a required key is missing.  Key: " + field.name());
                    }
                    bb = null;
                }
                requiredKeysToFind.remove(field.name());
                row.addField(new CassandraDataField(ByteBufferUtil.wrap(field.name()), bb));
            }
        }

        if (this.timestampInfo.hasTimestamp()) {
            final ByteBuffer bb = this.timestampInfo.isSaveAsLongType()
                    ? LongType.instance.decompose(Long.parseLong(this.timestampInfo.getTimestamp().get()))
                    : ByteBufferUtil.wrap(this.timestampInfo.getTimestamp().get());
            row.addField(
                    new CassandraDataField(ByteBufferUtil.wrap(SchemaUtil.DISPERSAL_TIMESTAMP), bb));
        }

        if (!requiredKeysToFind.isEmpty()) {
            final Joiner joiner = Joiner.on(",");
            throw new MissingPropertyException(joiner.join(requiredKeysToFind));
        }

        return Collections.singletonList(new ConverterResult<>(row));
    }

    /**
     * Not all the fields in the Avro Schema will be converted to Cassandra fields.
     *
     * All required fields listed, however, must exist in the fields to convert
     * @param allFieldsToConvert
     * @param requiredFields
     */
    private void validate(final Set<String> allFieldsToConvert, final List<String> requiredFields) {
        if (!allFieldsToConvert.containsAll(requiredFields)) {
            final List<String> missingFields = requiredFields.
                    stream()
                    .filter(rf -> allFieldsToConvert.contains(this.requiredFields))
                    .collect(Collectors.toList());
            final Joiner joiner = Joiner.on(",");
            final String errMsg = String.format("Listed required fields are missing from the list of fields to convert."
                    + " Please check your job configuration.  Missing fields are: %s", joiner.join(missingFields));
            throw new JobRuntimeException(errMsg);
        }
    }
}
