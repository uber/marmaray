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

import com.google.common.base.Preconditions;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.HDFSSchemaServiceConfiguration;
import com.uber.marmaray.common.configuration.HadoopConfiguration;
import com.uber.marmaray.common.exceptions.InvalidDataException;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.schema.HDFSSchemaService.HDFSSchemaServiceReader;
import com.uber.marmaray.common.schema.HDFSSchemaService.HDFSSchemaServiceWriter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Implementation of SchemaService that reads from HDFS. All schemas are assumed to exist in the PATH, in the format of
 * schemaName.schemaVersion.avsc.
 */
@Slf4j
public class HDFSSchemaService implements ISchemaService<HDFSSchemaServiceReader, HDFSSchemaServiceWriter>,
    Serializable {

    private static final String AVRO_SCHEMA_FILE_PATTERN = "%s.%d.avsc";
    private final Configuration conf;

    public HDFSSchemaService(@NonNull final Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Schema getWrappedSchema(@NotEmpty final String schemaName) {
        return getSchema(schemaName);
    }

    @Override
    public Schema getSchema(@NotEmpty final String schemaName) {
        final HadoopConfiguration hadoopConfiguration = new HadoopConfiguration(this.conf);
        final HDFSSchemaServiceConfiguration conf = new HDFSSchemaServiceConfiguration(this.conf);
        try {
            LocatedFileStatus resultSchemaFile = null;
            int resultSchemaVersion = -1;
            final FileSystem fileSystem = FileSystem.get(hadoopConfiguration.getHadoopConf());
            final RemoteIterator<LocatedFileStatus> fileIterator =
                fileSystem.listFiles(conf.getPath(), false);
            while (fileIterator.hasNext()) {
                final LocatedFileStatus f = fileIterator.next();
                final String schemaFile = f.getPath().getName();
                final String schemaFileSchemaName = getSchemaNameFromFileName(schemaFile);
                if (schemaFileSchemaName.equals(schemaName)) {
                    final int schemaVersion = getSchemaVersionFromFileName(schemaFile);
                    if (schemaVersion > resultSchemaVersion) {
                        resultSchemaFile = f;
                        resultSchemaVersion = schemaVersion;
                    }
                }
            }
            if (resultSchemaFile == null) {
                throw new JobRuntimeException(
                    String.format("Unable to find schema %s in %s", schemaName, conf.getPath()));
            } else {
                return getSchemaFromFile(resultSchemaFile);
            }
        } catch (IOException e) {
            throw new JobRuntimeException("Unable to load schema", e);
        }
    }

    /**
     * It fetches latest version of the schema.
     * @param schemaName Fully qualified schema name
     * @param schemaVersion schema version
     * @return Avro schema
     */
    public Schema getSchema(@NotEmpty final String schemaName, final int schemaVersion) {
        try {
            final HDFSSchemaServiceConfiguration conf = new HDFSSchemaServiceConfiguration(this.conf);
            Path schemaPath = new Path(conf.getPath(), String.format(AVRO_SCHEMA_FILE_PATTERN, schemaName,
                    schemaVersion));
            return getSchemaFromPath(schemaPath);
        } catch (IOException e) {
            throw new JobRuntimeException("Unable to load schema", e);
        }
    }

    private Schema getSchemaFromFile(@NonNull final LocatedFileStatus resultSchemaFile) throws IOException {
        return getSchemaFromPath(resultSchemaFile.getPath());
    }

    private Schema getSchemaFromPath(@NonNull final Path resultSchemaPath) throws IOException {
        final FileSystem fs = FileSystem.get(new HadoopConfiguration(this.conf).getHadoopConf());
        final FSDataInputStream inputStream = fs.open(resultSchemaPath);
        final String schemaString = IOUtils.toString(inputStream, UTF_8);
        return new Schema.Parser().parse(schemaString);
    }

    private int getSchemaVersionFromFileName(@NotEmpty final String schemaFile) {
        return Integer.valueOf(getFileParts(schemaFile)[1]);
    }

    private String[] getFileParts(@NotEmpty final String schemaFile) {
        final String[] parts = schemaFile.split("\\.");
        Preconditions.checkState(parts.length == 3, String.format("invalid file name %s", schemaFile));
        return parts;
    }

    private String getSchemaNameFromFileName(@NotEmpty final String schemaFile) {
        return getFileParts(schemaFile)[0];
    }

    @Override
    public HDFSSchemaServiceWriter getWriter(@NotEmpty final String schemaName, final int schemaVersion) {
        HDFSSchemaServiceWriter writer;
        try {
            final HDFSSchemaServiceConfiguration conf = new HDFSSchemaServiceConfiguration(this.conf);
            Path schemaPath = new Path(conf.getPath(), String.format(AVRO_SCHEMA_FILE_PATTERN, schemaName,
                    schemaVersion));
            writer = new HDFSSchemaServiceWriter(getSchemaFromPath(schemaPath));
        } catch (IOException e) {
            throw new JobRuntimeException("Unable to load schema", e);
        }
        return writer;
    }

    /**
     * It returns a writer with latest schema
     * @param schemaName Fully qualified schema name
     * @return An instance of {@link HDFSSchemaServiceWriter}
     */
    public HDFSSchemaServiceWriter getWriter(@NotEmpty final String schemaName) {
        return new HDFSSchemaServiceWriter(getSchema(schemaName));
    }

    @Override
    public HDFSSchemaServiceReader getReader(@NotEmpty final String schemaName, final int schemaVersion) {
        HDFSSchemaServiceReader reader;
        try {
            final HDFSSchemaServiceConfiguration conf = new HDFSSchemaServiceConfiguration(this.conf);
            Path schemaPath = new Path(conf.getPath(), String.format(AVRO_SCHEMA_FILE_PATTERN, schemaName,
                    schemaVersion));
            reader = new HDFSSchemaServiceReader(getSchemaFromPath(schemaPath));
        } catch (IOException e) {
            throw new JobRuntimeException("Unable to load schema", e);
        }
        return reader;
    }

    /**
     * It returns a reader with latest schema
     * @param schemaName Fully qualified schema name
     * @return An instance of {@link HDFSSchemaServiceReader}
     */
    public HDFSSchemaServiceReader getReader(@NotEmpty final String schemaName) {
        return new HDFSSchemaServiceReader(getSchema(schemaName));
    }

    public final class HDFSSchemaServiceReader implements ISchemaService.ISchemaServiceReader, Serializable {

        private final String schemaString;
        private transient Schema schema;

        public HDFSSchemaServiceReader(@NotEmpty final Schema schema) {
            this.schemaString = schema.toString();
            this.schema = schema;
        }

        public Schema getSchema() {
            if (this.schema == null) {
                this.schema = new Schema.Parser().parse(this.schemaString);
            }
            return this.schema;
        }

        @Override
        public GenericRecord read(final byte[] buffer) throws InvalidDataException {
            final DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(getSchema());
            final ByteArrayInputStream stream = new ByteArrayInputStream(buffer);
            stream.reset();
            final BinaryDecoder binaryDecoder = new DecoderFactory().binaryDecoder(stream, null);
            try {
                return datumReader.read(null, binaryDecoder);
            } catch (IOException e) {
                throw new InvalidDataException("Error decoding data", e);
            }
        }
    }
    public final class HDFSSchemaServiceWriter implements ISchemaService.ISchemaServiceWriter, Serializable {
        private final String schemaString;
        private transient Schema schema;

        public HDFSSchemaServiceWriter(@NotEmpty final Schema schema) {
            this.schemaString = schema.toString();
            this.schema = schema;
        }

        public Schema getSchema() {
            if (this.schema == null) {
                this.schema = new Schema.Parser().parse(this.schemaString);
            }
            return this.schema;
        }

        @Override
        public byte[] write(@NonNull final GenericRecord record) throws InvalidDataException {
            final SpecificDatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(getSchema());
            final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            byteArrayOutputStream.reset();
            final BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);
            try {
                datumWriter.write(record, binaryEncoder);
                binaryEncoder.flush();
            } catch (Exception e) {
                throw new InvalidDataException("Error encoding record", e);
            }
            return byteArrayOutputStream.toByteArray();
        }
    }
}
