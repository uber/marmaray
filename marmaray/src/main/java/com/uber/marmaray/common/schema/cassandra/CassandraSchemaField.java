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
package com.uber.marmaray.common.schema.cassandra;

import com.google.common.base.Joiner;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.utilities.GenericRecordUtil;
import com.uber.marmaray.utilities.StringTypes;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.spark.sql.execution.columnar.LONG;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.Serializable;

@AllArgsConstructor
public final class CassandraSchemaField implements Serializable {
    public static final String STRING_TYPE = "text";
    public static final String INT_TYPE = "int";
    public static final String LONG_TYPE = "bigint";
    public static final String FLOAT_TYPE = "float";
    public static final String BOOLEAN_TYPE = "boolean";
    public static final String DOUBLE_TYPE = "double";

    private static final Joiner joiner = Joiner.on(StringTypes.COMMA);

    @Getter
    @NotEmpty
    private final String fieldName;

    @Getter
    @NotEmpty
    private final String type;

    public String toString() {
        return this.fieldName + StringTypes.SPACE + type;
    }

    public static String convertFromAvroType(final Schema schema) {
        // If schema is a UNION type we need to take the non-nullable schema for schema backwards compatibility
        final Schema nonNullSchema = GenericRecordUtil.isOptional(schema) ? GenericRecordUtil.getNonNull(schema)
                : schema;
        final Schema.Type type = nonNullSchema.getType();

        switch (type) {
            case STRING:
                return STRING_TYPE;
            case INT:
                return INT_TYPE;
            case LONG:
                return LONG_TYPE;
            case FLOAT:
                return FLOAT_TYPE;
            case BOOLEAN:
                return BOOLEAN_TYPE;
            case DOUBLE:
                return DOUBLE_TYPE;
            default:
                // todo T935985: Support more complex types from Schema.Type
                throw new JobRuntimeException("Type " + type + " is not supported for conversion. Field(s): "
                        + joiner.join(schema.getFields()));
        }
    }
}
