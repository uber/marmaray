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
import com.google.common.collect.Maps;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.CassandraSinkConfiguration;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.converters.converterresult.ConverterResult;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.exceptions.MissingPropertyException;
import com.uber.marmaray.common.metrics.DataFeedMetricNames;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.ErrorCauseTagNames;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.common.metrics.ModuleTagNames;
import com.uber.marmaray.common.schema.cassandra.CassandraDataField;
import com.uber.marmaray.common.schema.cassandra.CassandraPayload;
import com.uber.marmaray.common.schema.cassandra.CassandraSchema;
import com.uber.marmaray.utilities.ByteBufferUtil;
import com.uber.marmaray.utilities.ErrorExtractor;
import com.uber.marmaray.utilities.GenericRecordUtil;
import com.uber.marmaray.utilities.SchemaUtil;
import com.uber.marmaray.utilities.TimeUnitUtil;
import com.uber.marmaray.utilities.TimestampInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link CassandraSinkDataConverter} extends {@link SinkDataConverter}
 * This class converts data from intermediate Avro schema to the Cassandra schema.  This
 * class is only to be used where the sink of the data migration is Cassandra.  The main convert method of this
 * class will return a RDD of Cassandra payloads to the caller.  All keys & values will be serialized as
 * ByteBuffers as required by Cassandra.  All strings are encoded using UTF-8.
 */
@Slf4j
public class CassandraSinkDataConverter extends SinkDataConverter<CassandraSchema, CassandraPayload> {

    private static final long serialVersionUID = 1L;
    private static final String orderTimestampFieldName = "orderTsField";
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
    private final TimestampInfo timestampInfo;
    private final CassandraSinkConfiguration cassandraConf;
    private final Optional<String> writtenTime;
    @Getter
    private Optional<DataFeedMetrics> dataFeedMetrics = Optional.absent();

    /**
     * This map (and schema) are created once per executor in the {@link #convert(AvroPayload)} method.
     * It cannot be initialized in constructor due to Schema.Field not being serializable.
     * This map caches per field information to avoid recalculations on each row.
     */
    private Optional<Map<Schema.Field, FieldInfo>> fieldInfoMap = Optional.absent();
    private Optional<Schema> inputSchemaAvro = Optional.absent();

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
                                      @NonNull final Optional<String> writtenTime,
                                      @NonNull final List<String> requiredFields,
                                      @NonNull final TimestampInfo timestampInfo,
                                      @NonNull final ErrorExtractor errorExtractor) {
        super(conf, errorExtractor);
        if (fieldsToConvert.isPresent()) {
            validate(fieldsToConvert.get(), requiredFields);
        }
        this.requiredFields = Collections.unmodifiableList(requiredFields);
        this.fieldsToConvert = fieldsToConvert;
        this.writtenTime = writtenTime;
        this.timestampInfo = timestampInfo;
        this.cassandraConf = new CassandraSinkConfiguration(conf);
        this.inputSchemaJson = inputSchema.toString();
    }

    @Override
    public void setDataFeedMetrics(@NonNull final DataFeedMetrics dataFeedMetrics) {
        this.dataFeedMetrics = Optional.of(dataFeedMetrics);
    }

    @Override
    public void setJobMetrics(@NonNull final JobMetrics jobMetrics) {
        // ignored
    }

    @Override
    public List<ConverterResult<AvroPayload, CassandraPayload>> convert(@NonNull final AvroPayload avroPayload) {
        final CassandraPayload row = new CassandraPayload();
        final Set<String> requiredKeysToFind =  new HashSet<>(this.requiredFields);
        if (!fieldInfoMap.isPresent()) {
            this.inputSchemaAvro = Optional.of(new Schema.Parser().parse(inputSchemaJson));
            this.fieldInfoMap = Optional.of(createFieldInfoMap(inputSchemaAvro.get().getFields()));
        }

        // Record the order column value for USING TIMESTAMP field.
        Optional<Long> lastbb = Optional.absent();
        Optional<String> lastFieldFname = Optional.absent();

        final GenericRecord genericRecord = avroPayload.getData();
        for (int i = 0; i < this.inputSchemaAvro.get().getFields().size(); i++) {
            final Schema.Field field = this.inputSchemaAvro.get().getFields().get(i);
            if (!this.fieldsToConvert.isPresent()
                    || this.fieldsToConvert.isPresent()
                    && this.fieldsToConvert.get().contains(field.name().toLowerCase())) {
                final Object rawData = genericRecord.get(field.name());

                final ByteBuffer bb;

                if (rawData != null) {
                    bb = getByteBuffer(field, rawData, this.dataFeedMetrics);
                    if (this.writtenTime.isPresent() && field.name().equals(this.writtenTime.get())) {
                        lastFieldFname = Optional.fromNullable(field.name());
                        lastbb = recordWrittenTime(getNonNullType(field), rawData);
                    }
                } else {
                    if (requiredKeysToFind.contains(field.name())) {
                        if (this.dataFeedMetrics.isPresent()) {
                            this.dataFeedMetrics.get()
                                    .createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_ERROR, 1,
                                            DataFeedMetricNames.getErrorModuleCauseTags(
                                                    ModuleTagNames.SINK_CONVERTER, ErrorCauseTagNames.MISSING_DATA));
                        }
                        continue;
                    }
                    bb = null;
                }
                if (requiredKeysToFind.contains(field.name())
                      && bb.limit() != 0) {
                    requiredKeysToFind.remove(field.name());
                }
                row.addField(new CassandraDataField(ByteBufferUtil.wrap(field.name()), bb));
            }
        }

        if (this.timestampInfo.hasTimestamp()) {
            final ByteBuffer bb = this.timestampInfo.isSaveAsLongType()
                    ? LongType.instance.decompose(Long.parseLong(this.timestampInfo.getTimestamp().get()))
                    : ByteBufferUtil.wrap(this.timestampInfo.getTimestamp().get());
            row.addField(
                    new CassandraDataField(ByteBufferUtil.wrap(this.timestampInfo.getTimestampFieldName()), bb));
        }

        if (!requiredKeysToFind.isEmpty()) {
            final String missingKeys = Joiner.on(",").join(requiredKeysToFind);
            if (this.cassandraConf.getShouldSkipInvalidRows()) {
                return Collections.singletonList(
                  new ConverterResult<>(
                    avroPayload,
                    String.format("Required keys are missing. Keys: %s", missingKeys)));
            }
            if (this.dataFeedMetrics.isPresent()) {
                this.dataFeedMetrics.get().createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_ERROR, 1,
                        DataFeedMetricNames.getErrorModuleCauseTags(
                                ModuleTagNames.SINK_CONVERTER, ErrorCauseTagNames.MISSING_FIELD));
            }
            throw new MissingPropertyException(missingKeys);
        }

        // Concatenate orderTimestamp value with tailing zero to meet the length of microseconds.
        if (lastFieldFname.isPresent() && lastbb.isPresent()) {
            row.addField(new CassandraDataField(ByteBufferUtil.wrap(orderTimestampFieldName),
                    LongType.instance.decompose(Long.parseLong(TimeUnitUtil.convertToMicroSeconds(lastbb.get())))));
        }

        return Collections.singletonList(new ConverterResult<>(row));
    }

    @NonNull
    private Map<Schema.Field, FieldInfo>  createFieldInfoMap(@NonNull final List<Schema.Field> fields) {

        final Map<Schema.Field, FieldInfo> fieldInfoMap = Maps.newHashMapWithExpectedSize(fields.size());
        for (final Schema.Field field : fields) {
            final Schema.Type type = getNonNullType(field);
            final boolean isTimestampField = SchemaUtil.isTimestampSchema(field.schema());
            final FieldInfo fieldInfo = new FieldInfo(type, isTimestampField);
            fieldInfoMap.put(field, fieldInfo);
        }
        return fieldInfoMap;
    }

    /**
     * Utility method to load the byte buffer from the message
     * @param field Schema.Field loaded from the object (this is how we know how to convert)
     * @param rawData the item extracted from the object
     * @return ByteBuffer of the bytes
     */
    private ByteBuffer getByteBuffer(@NonNull final Schema.Field field, @NonNull final Object rawData,
                                            @NonNull final Optional<DataFeedMetrics> metrics) {
        final ByteBuffer bb;
        final FieldInfo fieldInfo = fieldInfoMap.get().get(field);
        final Schema.Type type = fieldInfo.getNonNullType();
        if (fieldInfo.isTimestampField()) {
            final Timestamp ts = SchemaUtil.decodeTimestamp(rawData);
            final long longData = ts.getTime();
            bb = LongType.instance.decompose(longData);
        } else {
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
                case BYTES:
                    bb = (ByteBuffer) rawData;
                    break;
                // todo(T936057) - add support for non-primitive types
                default:
                    if (metrics.isPresent()) {
                        metrics.get()
                                .createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_ERROR, 1,
                                        DataFeedMetricNames.getErrorModuleCauseTags(
                                                ModuleTagNames.SINK_CONVERTER,
                                                ErrorCauseTagNames.NOT_SUPPORTED_FIELD_TYPE));
                    }
                    throw new JobRuntimeException(String.format("Type %s not supported for field %s",
                            field.schema().getType(), field.name()));
            }
        }
        return bb;
    }

    private static Schema.Type getNonNullType(@NonNull final Schema.Field field) {
        final Schema nonNullSchema = GenericRecordUtil.isOptional(field.schema())
                ? GenericRecordUtil.getNonNull(field.schema())
                : field.schema();
        return nonNullSchema.getType();
    }

    /**
     * Not all the fields in the Avro Schema will be converted to Cassandra fields.
     *
     * All required fields listed, however, must exist in the fields to convert
     * @param allFieldsToConvert
     * @param requiredFields
     */
    private void validate(@NonNull final Set<String> allFieldsToConvert, @NonNull final List<String> requiredFields) {
        if (!allFieldsToConvert.containsAll(requiredFields)) {
            final List<String> missingFields = requiredFields.
                    stream()
                    .filter(rf -> allFieldsToConvert.contains(this.requiredFields))
                    .collect(Collectors.toList());
            final Joiner joiner = Joiner.on(",");
            if (this.dataFeedMetrics.isPresent()) {
                this.dataFeedMetrics.get().createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_ERROR, 1,
                        DataFeedMetricNames.getErrorModuleCauseTags(
                                ModuleTagNames.SINK_CONVERTER, ErrorCauseTagNames.MISSING_FIELD));
            }
            final String errMsg = String.format("Listed required fields are missing from the list of fields to convert."
                    + " Please check your job configuration.  Missing fields are: %s", joiner.join(missingFields));
            throw new JobRuntimeException(errMsg);
        }
    }

    //TODO: Support Hive timestamp type.
    private Optional<Long> recordWrittenTime(@NonNull final Schema.Type type, @NonNull final Object rawData) {
        switch (type) {
            case LONG:
                return Optional.fromNullable((Long) rawData);
            case STRING:
                return Optional.fromNullable(Long.parseLong(rawData.toString()));
            default:
                if (this.dataFeedMetrics.isPresent()) {
                    this.dataFeedMetrics.get()
                            .createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_ERROR, 1,
                                    DataFeedMetricNames.getErrorModuleCauseTags(
                                            ModuleTagNames.SINK_CONVERTER,
                                            ErrorCauseTagNames.NOT_SUPPORTED_FIELD_TYPE));
                }
                throw new JobRuntimeException("Order column type " + type
                        + " not supported. Only LONG and STRING type are supported.");
        }
    }

    private class FieldInfo {
        final Schema.Type nonNullType;
        final boolean isTimestampField;

        FieldInfo(@NonNull final Schema.Type nonNullType, final boolean isTimestampField) {
            this.nonNullType = nonNullType;
            this.isTimestampField = isTimestampField;
        }

        Schema.Type getNonNullType() {
            return nonNullType;
        }

        boolean isTimestampField() {
            return isTimestampField;
        }
    }
}
