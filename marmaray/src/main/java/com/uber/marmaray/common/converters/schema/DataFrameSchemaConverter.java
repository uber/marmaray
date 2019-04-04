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
package com.uber.marmaray.common.converters.schema;

import com.google.common.base.Preconditions;
import com.uber.marmaray.utilities.SchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * {@link DataFrameSchemaConverter} extends {@AbstractSchemaConverter} and converts {@link StructType}
 * to {@link Schema}
 *
 * Conversion from {@link Schema} to {@link StructType} is currently not supported.
 *
 */
public class DataFrameSchemaConverter extends AbstractSchemaConverter<StructType, Schema> {

    @Override
    public Schema convertToCommonSchema(final StructType dataFrameSchema) {
        Preconditions.checkNotNull(dataFrameSchema);
        final SchemaBuilder.FieldAssembler<Schema> fieldAssembler =  SchemaBuilder.record("commonSchema").fields();
        for (final StructField sf : dataFrameSchema.fields()) {
            addField(fieldAssembler, sf);
        }

        return fieldAssembler.endRecord();
    }

    @Override
    public StructType convertToExternalSchema(final Schema commonSchema) {
        // We don't anticipate needing this currently but if needed we can implement
        throw new UnsupportedOperationException();
    }

    private void addField(final SchemaBuilder.FieldAssembler<Schema> fieldAssembler, final StructField structField) {
        final SchemaBuilder.FieldBuilder<Schema> fieldBuilder = fieldAssembler.name(structField.name());

        final DataType dt = structField.dataType().defaultConcreteType();

        /*
         * Can't use switch statement here because this is not a compile time constant expression
         * and I'd rather compare it to a concrete type rather than use strings
         *
         * For now, we just handle atomic, fractional, or integral types.
         * Todo: Handle Map/Array/Struct types etc
         */
        if (dt.equals(DataTypes.TimestampType)) {
            fieldBuilder.type(SchemaUtil.getTimestampSchema(structField.nullable())).noDefault();
        } else {
            final SchemaBuilder.BaseFieldTypeBuilder<Schema> fieldType =
                structField.nullable() ? fieldBuilder.type().nullable() : fieldBuilder.type();
            if (dt.equals(DataTypes.StringType)) {
                fieldType.stringType().noDefault();
            } else if (dt.equals(DataTypes.BooleanType)) {
                fieldType.booleanType().noDefault();
            } else if (dt.equals(DataTypes.DateType)) {
                fieldType.stringType().noDefault();
            } else if (dt.equals(DataTypes.BinaryType)) {
                fieldType.bytesType().noDefault();
            } else if (dt.equals(DataTypes.DoubleType)) {
                fieldType.doubleType().noDefault();
            } else if (dt.equals(DataTypes.FloatType)) {
                fieldType.floatType().noDefault();
            } else if (dt.equals(DataTypes.ByteType)) {
                fieldType.bytesType().noDefault();
            } else if (dt.equals(DataTypes.IntegerType)) {
                fieldType.intType().noDefault();
            } else if (dt.equals(DataTypes.LongType)) {
                fieldType.longType().noDefault();
            } else if (dt.equals(DataTypes.ShortType)) {
                // no corresponding short type in DataTypes
                // we can make this int and lose no precision
                fieldType.intType().noDefault();
            } else {
                throw new RuntimeException("The field type " + dt + " is not supported");
            }
        }
        /*
         * Todo: Handle following types
         * CalendarIntervalType
         * StructType
         * MapType
         * ArrayType
         * NullType
         *
         */
    }
}
