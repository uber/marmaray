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

import com.google.common.base.Optional;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.execution.datasources.parquet.ParquetSchemaConverter;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.IOException;

/**
 * {@link SchemaUtil} defines utility methods for working with schemas
 */
@Slf4j
public final class SchemaUtil {

    public static final String DISPERSAL_TIMESTAMP = "dispersal_timestamp";

    private SchemaUtil() {
        throw new JobRuntimeException("This utility class should never be instantiated");
    }

    /**
     * This utility method will iterate through a directory containing parquet files, find the first file,
     * and only read in the Parquet metadata and convert the parquet schema to the equivalent Spark StructType.
     *
     * This method is useful because it does not require reading in all the data into memory to determine the schema
     * and only reads in the required metadata located in the footer
     * @param parquetDir
     * @return StructType equivalent of the parquet schema
     * @throws IOException
     */
    public static StructType generateSchemaFromParquet(@NonNull final FileSystem fs,
                                                       @NotEmpty final String parquetDir) throws IOException {

        log.info("Searching {} for parquet files", parquetDir);

        final FileStatus[] fileStatuses = fs.listStatus(new Path(parquetDir));

        Optional<Path> parquetFilePath = Optional.absent();

        final FileStatus lastEntry = fileStatuses[fileStatuses.length - 1];

        if (lastEntry.isFile()) {
            log.info("Reading from last FileStatus object: {}", lastEntry.getPath());
            parquetFilePath = Optional.of(lastEntry.getPath());
        } else if (lastEntry.isDirectory()) {

            final FileStatus[] directoryEntries = fs.listStatus(lastEntry.getPath());

            final FileStatus lastFile = directoryEntries[directoryEntries.length - 1];

            if (lastFile.isFile()) {
                log.info("Reading schema data from : {}", lastFile.getPath().toString());
                parquetFilePath = Optional.of(lastFile.getPath());
            } else {
                throw new JobRuntimeException("Encountered a directory where there should only be files. Path: "
                        + lastFile.getPath().toString());
            }
        }

        if (!parquetFilePath.isPresent()) {
            throw new JobRuntimeException("No files were found in path: " + parquetDir);
        }

        log.info("Reading parquet file: {} for schema", parquetFilePath.get());

        final ParquetMetadata metadata =
                ParquetFileReader.readFooter(new Configuration(),
                        parquetFilePath.get(), ParquetMetadataConverter.NO_FILTER);
        final MessageType messageType = metadata.getFileMetaData().getSchema();
        final ParquetSchemaConverter converter = new ParquetSchemaConverter(new SQLConf());
        final StructType structType = converter.convert(messageType);
        return structType;
    }
}
