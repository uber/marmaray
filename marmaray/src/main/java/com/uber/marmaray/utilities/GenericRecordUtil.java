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
package com.uber.marmaray.utilities;

import com.google.common.base.Preconditions;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link GenericRecordUtil} defines utility methods for working with Generic Records
 */
@Slf4j
public final class GenericRecordUtil {

    private GenericRecordUtil() {
        throw new JobRuntimeException("Utility class should not be instantiated");
    }

    /**
     * @return true if given schema is a union schema and is nullable
     */
    public static boolean isOptional(@NonNull final Schema schema) {
        return schema.getType().equals(Schema.Type.UNION)
            && (schema.getTypes().size() == 2)
            && (schema.getTypes().get(0).getType().equals(Schema.Type.NULL)
            || schema.getTypes().get(1).getType().equals(Schema.Type.NULL));
    }

    /**
     * @return non null schema. Make sure that {@link #isOptional(Schema)} returns true for the given
     * schema.
     */
    public static Schema getNonNull(@NonNull final Schema schema) {
        final List<Schema> types = schema.getTypes();
        Preconditions.checkState(types.size() == 2, "not an optional schema");
        return types.get(0).getType().equals(Schema.Type.NULL) ? types.get(1) : types.get(0);
    }

    /**
     * @return true if both records match
     */
    public static boolean compareRecords(final GenericRecord record1, final GenericRecord record2) {
        if (record1 == null || record2 == null) {
            return record1 == record2;
        } else {
            for (final Schema.Field f : record1.getSchema().getFields()) {
                final String name = f.name();
                final Object value = record1.get(name);
                final Object newValue = record2.get(name);
                if (value == null || newValue == null) {
                    if (value != newValue) {
                        return false;
                    }
                    continue;
                }
                if (!compareFields(value, newValue, f.schema())) {
                    return false;
                }
            }
            return true;
        }
    }

    private static boolean compareFields(final Object field1, final Object field2, @NonNull final Schema schema) {
        final Schema nonNullSchema;
        if (isOptional(schema)) {
            nonNullSchema = getNonNull(schema);
        } else {
            nonNullSchema = schema;
        }
        if (field1 == null || field2 == null) {
            return field1 == field2;
        }
        switch (nonNullSchema.getType()) {
            case BOOLEAN:
            case INT:
            case DOUBLE:
            case FLOAT:
            case LONG:
            case STRING:
                return field1.equals(field2);
            case RECORD:
                return compareRecords((GenericRecord) field1, (GenericRecord) field2);
            case ENUM:
                if (!field1.toString().equals(field2.toString())) {
                    return false;
                }
                return true;
            case ARRAY:
                final Schema elementSchema = nonNullSchema.getElementType();
                final List list1 = (List) field1;
                final List list2 = (List) field2;
                if (list1.size() != list2.size()) {
                    return false;
                }
                for (int idx = 0; idx < list1.size(); idx++) {
                    if (!compareFields(list1.get(idx), list2.get(idx), elementSchema)) {
                        return false;
                    }
                }
                return true;
            case MAP:
                final Schema valueSchema = nonNullSchema.getValueType();
                final Map<String, Object> map1 = (Map<String, Object>) field1;
                final Map<String, Object> map2 = (Map<String, Object>) field2;
                if (map1.size() != map2.size()) {
                    return false;
                }
                for (String key : map1.keySet()) {
                    if (!map2.containsKey(key)) {
                        return false;
                    }
                    if (!compareFields(map1.get(key), map2.get(key), valueSchema)) {
                        return false;
                    }
                }
                return true;
            default:
                throw new JobRuntimeException("Invalid data type");
        }
    }

    /**
     * Helper method to fix {@link org.apache.avro.Schema.Type#ENUM} datatypes in {@link GenericRecord}.
     */
    public static GenericRecord fixEnumRecord(@NonNull final GenericRecord record, @NonNull final Schema schema)
        throws IOException {
        final GenericRecord retRecord = new Record(schema);
        for (final Schema.Field f : schema.getFields()) {
            final String name = f.name();
            retRecord.put(f.name(), fixEnumRecordField(record.get(name), f.name(), f.schema()));
        }
        return retRecord;
    }

    private static Object fixEnumRecordField(final Object value, @NotEmpty final String name,
        @NonNull final Schema schema) throws IOException {
        log.debug("name:{}:value:{}:schema:{}", name, value, schema);
        final Schema nonOptionalSchema;
        if (isOptional(schema)) {
            if (value == null) {
                return null;
            } else {
                nonOptionalSchema = getNonNull(schema);
            }
        } else if (value == null) {
            // Always fail on null for non-nullable schemas
            throw new JobRuntimeException(String.format("value cannot be null :name {} : schema : {}",
                name, schema));
        } else {
            nonOptionalSchema = schema;
        }
        switch (nonOptionalSchema.getType()) {
            case BOOLEAN:
                if (value instanceof Boolean) {
                    return ((Boolean) value).booleanValue();
                }
                throw new JobRuntimeException(
                    "data type is not boolean :" + name + " : type :" + value.getClass().getCanonicalName()
                        + ": value :" + value.toString());
            case DOUBLE:
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                throw new JobRuntimeException(
                    "data type is not double :" + name + " : type :" + value.getClass().getCanonicalName() + ": value :"
                        + value.toString());
            case FLOAT:
                if (value instanceof Number) {
                    return ((Number) value).floatValue();
                }
                throw new JobRuntimeException(
                    "data type is not float :" + name + " : type :" + value.getClass().getCanonicalName() + ": value :"
                        + value.toString());
            case INT:
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }
                throw new JobRuntimeException(
                    "data type is not int :" + name + " : type :" + value.getClass().getCanonicalName() + ": value :"
                        + value.toString());
            case LONG:
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                throw new JobRuntimeException(
                    "data type is not long :" + name + " : type :" + value.getClass().getCanonicalName() + ": value :"
                        + value.toString());
            case STRING:
                return value.toString();
            case BYTES:
                return value;
            case RECORD:
                return fixEnumRecord((GenericRecord) value, nonOptionalSchema);
            case ENUM:
                if (nonOptionalSchema.getEnumSymbols().contains(value.toString())) {
                    return new GenericData.EnumSymbol(nonOptionalSchema, value.toString());
                }
                throw new JobRuntimeException(String.format("Invalid symbol - symbol :{} , schema :{}",
                    value.toString(), nonOptionalSchema.toString()));
            case ARRAY:
                final Schema elementSchema = nonOptionalSchema.getElementType();
                final List listRes = new ArrayList();
                final List inputList = (List) value;
                if (inputList.contains(null)) {
                    return listRes;
                }
                for (final Object v : inputList) {
                    try {
                        switch (elementSchema.getType()) {
                            case BOOLEAN:
                            case DOUBLE:
                            case FLOAT:
                            case INT:
                            case LONG:
                            case STRING:
                            case ENUM:
                            case BYTES:
                            case RECORD:
                                log.debug("name:{}:v:{}:v.class:{}:elementSchema:{}", name, v,
                                    v.getClass().getCanonicalName(), elementSchema);
                                if (v instanceof Record && ((Record) v).get("array") != null) {
                                    listRes.add(fixEnumRecordField(((Record) v).get("array"), name, elementSchema));
                                } else {
                                    listRes.add(fixEnumRecordField(v, name, elementSchema));
                                }
                                break;
                            default:
                                throw new JobRuntimeException("invalid array element's data type :"
                                    + v.getClass().getCanonicalName());
                        }
                    } catch (Exception e) {
                        log.error("elementSchema:{}:value:{}:valueClass:{}:v:{}:v.getClass{}", elementSchema, value,
                            value.getClass().getCanonicalName(), v, v.getClass().getCanonicalName());
                        throw e;
                    }
                }
                return listRes;
            case MAP:
                final Schema valueSchema = nonOptionalSchema.getValueType();
                Map<String, Object> ret = new HashMap<>();
                for (final Map.Entry<String, Object> v : ((Map<String, Object>) value).entrySet()) {
                    ret.put(v.getKey(), fixEnumRecordField(v.getValue(), name, valueSchema));
                }
                return ret;
            default:
                log.error("name:{}:value:{}:valueClass:{}:schema:{}", name, value,
                    value == null ? "" : value.getClass(), schema);
                throw new JobRuntimeException("unsupported data type :" + nonOptionalSchema.getType());
        }
    }
}
