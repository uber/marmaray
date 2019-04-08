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

import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.marmaray.common.HoodieErrorPayload;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.ErrorTableConfiguration;
import com.uber.marmaray.common.configuration.HoodieConfiguration;
import com.uber.marmaray.common.converters.data.DummyHoodieSinkDataConverter;
import com.uber.marmaray.common.data.ErrorData;
import com.uber.marmaray.common.data.RDDWrapper;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.metadata.HoodieBasedMetadataManager;
import com.uber.marmaray.common.sinks.hoodie.HoodieErrorSink;
import com.uber.marmaray.common.sinks.hoodie.HoodieSink;
import com.google.common.base.Optional;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.hibernate.validator.constraints.NotEmpty;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.uber.marmaray.common.configuration.SparkConfiguration.SPARK_PROPERTIES_KEY_PREFIX;
import static com.uber.marmaray.utilities.DateUtil.DATE_PARTITION_FORMAT;

/**
 * {@link ErrorTableUtil} defines utility methods to interact with the error tables
 */
@Slf4j
public final class ErrorTableUtil {

    public static final String HADOOP_ROW_KEY = "Hadoop_Row_Key";
    public static final String HADOOP_ERROR_SOURCE_DATA = "hadoop_error_source_data";
    public static final String HADOOP_ERROR_EXCEPTION = "hadoop_error_exception";
    public static final String HADOOP_CHANGELOG_COLUMNS = "Hadoop_Changelog_Columns";
    public static final String HADOOP_APPLICATION_ID = "hadoop_application_id";
    public static final String HOODIE_RECORD_KEY = "Hoodie_record_key_constant_%s";
    public static final String ERROR_SCHEMA_IDENTIFIER = "spark.user.ERROR_SCHEMA";
    public static final String ERROR_TABLE_SUFFIX = "_error";
    public static final String TABLE_KEY = "spark.user.table_key";
    public static final String ERROR_TABLE_KEY = "spark.user.error_table_key";
    public static final Integer ERROR_ROW_KEY_SUFFIX_MAX = 256;
    public static final Integer ERROR_TABLE_RANDOM_SEED_VALUE = 1;
    /**
     * Default flag to control whether error table metrics is enabled
     */
    public static final boolean ERROR_METRICS_IS_ENABLED = false;

    private ErrorTableUtil() {
        throw new JobRuntimeException("This is a utility class that should never be instantiated");
    }

    /**
     * Helper method to write to error table.
     *
     * @param sc        {@link SparkContext}
     * @param conf      {@link Configuration}
     * @param tableName Name of the table for which the current ingestion job ran. It will help in grouping error
     *                  records for a particular table.
     * @param errorData RDDWrapper of ErrorData(RDD) and count. It should have error message populated per record.
     */
    public static void writeErrorRecordsToErrorTable(@NonNull final SparkContext sc,
                                                     @NonNull final Configuration conf,
                                                     @NonNull final Optional<String> tableName,
                                                     @NonNull final RDDWrapper<ErrorData> errorData,
                                                     @NonNull final ErrorExtractor errorExtractor) {

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
        final ErrorTableConfiguration errorTableConf = new ErrorTableConfiguration(conf);
        if (!errorTableConf.isEnabled()) {
            return;
        }
        final long numErrors = errorData.getCount();
        log.info("number of Errors : {}", numErrors);
        if (numErrors == 0) {
            return;
        }
        String partitionPath = getPartitionPath();
        final String applicationId = jsc.getConf().getAppId();
        String errorTableSchema = sc.getConf().get(ERROR_SCHEMA_IDENTIFIER);
        String targeTable = sc.getConf().get(TABLE_KEY);
        String errorTable = sc.getConf().get(ERROR_TABLE_KEY);

        final AtomicBoolean shouldSaveChanges = new AtomicBoolean(true);
        HoodieConfiguration hoodieConf = errorTableConf.getHoodieConfiguration(conf, errorTableSchema, targeTable,
            errorTable, ERROR_METRICS_IS_ENABLED);

        try {
            final HoodieBasedMetadataManager metadataManager =
                new HoodieBasedMetadataManager(hoodieConf, shouldSaveChanges, jsc);
            final HoodieSink hoodieSink = new HoodieErrorSink(hoodieConf, new DummyHoodieSinkDataConverter(), jsc,
                                                                 HoodieSink.HoodieSinkOp.BULK_INSERT, metadataManager,
                                                                 false);

            JavaRDD<GenericRecord> errorRecords = errorData.getData().map(error -> generateGenericErrorRecord(
                errorExtractor, errorTableSchema, error, applicationId));

            JavaRDD<HoodieRecord<HoodieErrorPayload>> hoodieRecords = errorRecords.map(
                new Function<GenericRecord, HoodieRecord<HoodieErrorPayload>>() {

                    final Random randomRowKeySuffixGenerator = new Random(ERROR_TABLE_RANDOM_SEED_VALUE);

                    @Override
                    public HoodieRecord<HoodieErrorPayload> call(final GenericRecord genericRecord) {
                        final HoodieKey hoodieKey = new HoodieKey(
                            String.format(HOODIE_RECORD_KEY,
                                randomRowKeySuffixGenerator.nextInt(ERROR_ROW_KEY_SUFFIX_MAX)), partitionPath);
                        HoodieErrorPayload payload = new HoodieErrorPayload(genericRecord);
                        return new HoodieRecord<>(hoodieKey, payload);
                    }
                }
            );

            RDDWrapper<HoodieRecord<HoodieRecordPayload>> hoodieErrorRecords = new RDDWrapper(hoodieRecords, numErrors);
            hoodieSink.write(hoodieErrorRecords);
        } catch (IOException ioe) {
            final String errMessage = String.format("Failed to write error records for table:%s in application:%s",
                tableName, applicationId);
            log.error(errMessage, ioe);
            throw new JobRuntimeException(errMessage, ioe);
        }
    }

    public static void initErrorTableDataset(@NonNull final Configuration conf, @NotEmpty final String errorTableName)
        throws IOException {
        final ErrorTableConfiguration errorTableConf = new ErrorTableConfiguration(conf);
        final HoodieConfiguration hoodieConf = HoodieConfiguration.newBuilder(conf, errorTableName)
                                                   .withBasePath(errorTableConf.getDestPath().toString())
                                                   .withTableName(errorTableName)
                                                   .enableMetrics(false)
                                                   .build();
        HoodieUtil.initHoodieDataset(FSUtils.getFs(conf, Optional.of(hoodieConf.getBasePath())), hoodieConf);
    }

    public static void addErrorSchemaConfiguration(
        @NonNull final Configuration configuration, @NonNull final Schema errorSchema,
        @NotEmpty final String tableKey, @NotEmpty final String errorTableKey) {
        // Add Error schema, target table and error table for spark conf to be retrieved later
        configuration.setProperty(
            SPARK_PROPERTIES_KEY_PREFIX + ERROR_SCHEMA_IDENTIFIER, errorSchema.toString());
        configuration.setProperty(
            SPARK_PROPERTIES_KEY_PREFIX + TABLE_KEY, tableKey);
        configuration.setProperty(
            SPARK_PROPERTIES_KEY_PREFIX + ERROR_TABLE_KEY, errorTableKey);
    }

    private static GenericRecord generateGenericErrorRecord(@NonNull final ErrorExtractor errorExtractor,
                                                            @NotEmpty final String schema,
                                                            @NonNull final ErrorData error,
                                                            @NotEmpty final String applicationId) {
        Schema errorSchema = new Schema.Parser().parse(schema);
        GenericRecord newRecord = new GenericData.Record(errorSchema);
        newRecord.put(HADOOP_ROW_KEY.toLowerCase(), errorExtractor.getRowKey(error.getRawData()));
        newRecord.put(HADOOP_ERROR_SOURCE_DATA, errorExtractor.getErrorSourceData(error));
        newRecord.put(HADOOP_ERROR_EXCEPTION, errorExtractor.getErrorException(error));
        newRecord.put(HADOOP_CHANGELOG_COLUMNS.toLowerCase(), errorExtractor.getChangeLogColumns(error.getRawData()));
        newRecord.put(HADOOP_APPLICATION_ID, applicationId);
        return newRecord;
    }

    private static String getPartitionPath() {
        final ZonedDateTime date = ZonedDateTime.now(ZoneOffset.UTC);
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_PARTITION_FORMAT);
        return date.format(formatter);
    }
}
