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

import com.google.common.base.Optional;
import com.uber.marmaray.common.schema.cassandra.CassandraSchema;
import com.uber.marmaray.common.schema.cassandra.CassandraSchemaField;
import com.uber.marmaray.utilities.StringTypes;
import com.uber.marmaray.utilities.TimestampInfo;
import lombok.Getter;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.Set;

/**
 * {@link CassandraSchemaConverter} extends {@AbstractSchemaConverter} and converts {@link CassandraSchema}
 * to {@link Schema} and vice versa
 *
 * In Cassandra, all keys and values are generic ByteBuffers.
 *
 * The actual cassandra schema would just be a list of ByteBuffer that describes each of the field names
 * In order to encode the values of each row properly as ByteBuffers properly, however, we need to know
 * the type of field from the common schema
 */
public class CassandraSchemaConverter extends AbstractSchemaConverter<CassandraSchema, Schema> {
    @Getter
    final String keySpace;

    @Getter
    final String tableName;

    final TimestampInfo tsInfo;

    /**
     * This optional field is only populated when a job is configured to only handle a subset of fields
     * and not all the available fields from the source data.
     */
    final Optional<Set<String>> filteredFields;

    public CassandraSchemaConverter(@NotEmpty final String keySpace,
                                    @NotEmpty final String tableName,
                                    @NonNull final TimestampInfo timestampInfo,
                                    @NonNull final Optional<Set<String>> filteredFields) {
        this.keySpace = keySpace;
        this.tableName = tableName;
        this.tsInfo = timestampInfo;
        this.filteredFields = filteredFields;
    }

    public CassandraSchemaConverter(@NotEmpty final String keySpace,
                                    @NotEmpty final String tableName,
                                    @NonNull final Optional<Set<String>> filteredFields) {
        this(keySpace, tableName, TimestampInfo.generateEmptyTimestampInfo(), filteredFields);
    }

    @Override
    public CassandraSchema convertToExternalSchema(final Schema commonSchema) {
        // todo T936057 - Need to handle more complex schemas (i.e Record inside record)
        final CassandraSchema cs = new CassandraSchema(this.keySpace, this.tableName);

        for (final Schema.Field field : commonSchema.getFields()) {
            // Cassandra does not support field names starting with _
            if (this.shouldIncludeField(field.name())) {
                final String cassFieldType = CassandraSchemaField.convertFromAvroType(field.schema());
                cs.addField(new CassandraSchemaField(field.name(), cassFieldType));
            }
        }

        if (this.tsInfo.hasTimestamp()) {
            if (this.tsInfo.isSaveAsLongType()) {
                cs.addField(new CassandraSchemaField(tsInfo.getTimestampFieldName(), CassandraSchemaField.LONG_TYPE));
            } else {
                cs.addField(new CassandraSchemaField(tsInfo.getTimestampFieldName(), CassandraSchemaField.STRING_TYPE));
            }
        }

        return cs;
    }

    @Override
    public Schema convertToCommonSchema(final CassandraSchema schema) {
        throw new UnsupportedOperationException();
    }
    private boolean shouldIncludeField(@NotEmpty final String fieldName) {
        boolean shouldInclude = false;
        if (!fieldName.startsWith(StringTypes.UNDERSCORE)) {
            if (this.filteredFields.isPresent()) {
                if (this.filteredFields.get().contains(fieldName.toLowerCase())) {
                    shouldInclude = true;
                }
            } else {
                shouldInclude = true;
            }
        }
        return shouldInclude;
    }
}
