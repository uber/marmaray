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
package com.uber.marmaray.common.schema;

import com.uber.marmaray.common.exceptions.InvalidDataException;
import com.uber.marmaray.common.schema.ISchemaService.ISchemaServiceReader;
import com.uber.marmaray.common.schema.ISchemaService.ISchemaServiceWriter;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * The implementing class of {@link ISchemaService} provides support to fetch avro schema and serialize/deserialize
 * {@link GenericRecord}. Optionally implementations of it can encode raw bytes.
 * @param <R> Implementing class of {@link ISchemaServiceReader} provides implementation to deserialize
 * raw bytes into {@link GenericRecord}.
 * @param <W> Implementing class of {@link ISchemaServiceWriter} provides implementation to serialize
 * {@link GenericRecord} into raw bytes.
 */
public interface ISchemaService<R extends ISchemaServiceReader, W extends ISchemaServiceWriter> {

    /**
     * It fetches latest version of the schema and wraps it with custom wrapper fields.
     * @param schemaName Fully qualified schema name
     * @return Avro schema
     */
    Schema getWrappedSchema(@NotEmpty final String schemaName);

    /**
     * It fetches latest version of the schema.
     * @param schemaName Fully qualified schema name
     * @return Avro schema
     */
    Schema getSchema(@NotEmpty final String schemaName);

    /**
     * @param schemaName Fully qualified schema name
     * @param schemaVersion schema version
     * @return An instance of {@link ISchemaServiceWriter}
     */
    W getWriter(@NotEmpty final String schemaName, final int schemaVersion);

    /**
     * @param schemaName Fully qualified schema name
     * @param schemaVersion schema version
     * @return An instance of {@link ISchemaServiceReader}
     */
    R getReader(@NotEmpty final String schemaName, final int schemaVersion);

    /**
     * Implementing class of {@link ISchemaServiceReader} provides support to deserialize raw bytes into
     * {@link GenericRecord}. Optionally it can decode bytes if corresponding {@link ISchemaServiceWriter} used
     * encoded bytes during serialization.
     */
    interface ISchemaServiceReader {
        GenericRecord read(@NonNull final byte[] buffer) throws InvalidDataException;
    }

    /**
     * Implementing class of {@link ISchemaServiceWriter} provides support to serialize {@link GenericRecord} into
     * raw bytes. Optionally it can encode bytes.
     */
    interface ISchemaServiceWriter {
        byte[] write(@NonNull final GenericRecord record) throws InvalidDataException;
    }
}
