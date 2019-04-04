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
import com.google.common.base.Preconditions;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.metrics.DataFeedMetricNames;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.ErrorCauseTagNames;
import com.uber.marmaray.common.metrics.ModuleTagNames;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
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
import java.sql.Timestamp;

/**
 * {@link SchemaUtil} defines utility methods for working with schemas
 */
@Slf4j
public final class SchemaUtil {

    public static final String DISPERSAL_TIMESTAMP = "dispersal_timestamp";
    public static final String TIMESTAMP_PROPERTY = "timestamp";
    public static final String TRUE = "true";

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
                                                       @NotEmpty final String parquetDir,
                                                       @NonNull final Optional<DataFeedMetrics> dataFeedMetrics)
            throws IOException {

        log.info("Searching: {} for parquet files", parquetDir);
        // TODO : wrong path causes list status failure
        final FileStatus[] fileStatuses;
        final Path filePath = new Path(parquetDir);
        if (fs.exists(filePath)) {
            try {
                fileStatuses = fs.listStatus(filePath);
            } catch (Exception e) {
                log.error("Failed listing files to read, possible permission issue");
                if (dataFeedMetrics.isPresent()) {
                    dataFeedMetrics.get().createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_CONFIG_ERROR, 1,
                            DataFeedMetricNames.getErrorModuleCauseTags(
                                    ModuleTagNames.SOURCE, ErrorCauseTagNames.PERMISSON));
                }
                throw new JobRuntimeException("Failed at listing hive files", e);
            }
        } else {
            log.error("Data file path does not exist");
            if (dataFeedMetrics.isPresent()) {
                dataFeedMetrics.get().createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_CONFIG_ERROR, 1,
                        DataFeedMetricNames.getErrorModuleCauseTags(
                                ModuleTagNames.SOURCE, ErrorCauseTagNames.NO_FILE));
            }
            throw new JobRuntimeException("path does not exist: " + parquetDir);
        }
        if (fileStatuses.length == 0) {
            log.error("Data file path is empty");
            if (dataFeedMetrics.isPresent()) {
                dataFeedMetrics.get().createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_CONFIG_ERROR, 1,
                        DataFeedMetricNames.getErrorModuleCauseTags(
                                ModuleTagNames.SOURCE, ErrorCauseTagNames.EMPTY_PATH));
            }
            throw new JobRuntimeException("empty path:" + parquetDir);
        }

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
                // support multiple partitions
                log.info("Found another directory {}.", lastFile.getPath().getName());
                return generateSchemaFromParquet(fs, lastFile.getPath().toString(), dataFeedMetrics);
            }
        }

        if (!parquetFilePath.isPresent()) {
            log.error("No file was found in path: {}", parquetDir);
            if (dataFeedMetrics.isPresent()) {
                dataFeedMetrics.get().createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_CONFIG_ERROR, 1,
                        DataFeedMetricNames.getErrorModuleCauseTags(
                                ModuleTagNames.SOURCE, ErrorCauseTagNames.EMPTY_PATH));
            }
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

    /**
     * This utility will determine the internal version of timestamp.
     * Required as avro 1.7.7 doesn't support timestamps natively.
     * @param nullable if field is nullable
     * @return schema definition for timestamp
     */
    public static Schema getTimestampSchema(final boolean nullable) {
        if (nullable) {
            return SchemaBuilder.builder().nullable().longBuilder().prop(TIMESTAMP_PROPERTY, TRUE).endLong();
        } else {
            return SchemaBuilder.builder().longBuilder().prop(TIMESTAMP_PROPERTY, TRUE).endLong();

        }
    }

    /**
     * This utility will verify if the schema is representing a timestamp field.
     * Required as avro 1.7.7 doesn't support timestamps natively.
     * @param schema schema to verify
     * @return true if schema is representing a timetsamp
     */
    public static boolean isTimestampSchema(@NonNull final Schema schema) {
        final Schema nonNullSchema = GenericRecordUtil.isOptional(schema) ? GenericRecordUtil.getNonNull(schema)
            : schema;
        return Schema.Type.LONG.equals(nonNullSchema.getType())
            && TRUE.equals(nonNullSchema.getProp(TIMESTAMP_PROPERTY));
    }

    /**
     * Encode {@link Timestamp} into our internal format, ms since epoch
     * @param timestamp The timestamp to encode
     * @return the value that's encoded
     */
    public static Object encodeTimestamp(@NonNull final Timestamp timestamp) {
        return timestamp.getTime();
    }

    /**
     * Decode timestamp from our internal format, ms since epoch
     * @param objectTs the encoded value
     * @return the decoded {@link Timestamp} object
     */
    public static Timestamp decodeTimestamp(@NonNull final Object objectTs) {
        Preconditions.checkArgument(objectTs instanceof Long, "Invalid object to decode");
        return new Timestamp((Long) objectTs);
    }
}
