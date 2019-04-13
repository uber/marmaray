/*
 * Copyright (c) 2019 Uber Technologies, Inc.
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

package com.uber.marmaray.examples.job;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.uber.marmaray.common.configuration.CassandraMetadataManagerConfiguration;
import com.uber.marmaray.common.configuration.CassandraSinkConfiguration;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.HiveConfiguration;
import com.uber.marmaray.common.configuration.HiveSourceConfiguration;
import com.uber.marmaray.common.converters.data.CassandraSinkCQLDataConverter;
import com.uber.marmaray.common.converters.data.CassandraSinkDataConverter;
import com.uber.marmaray.common.converters.data.SparkSourceDataConverter;
import com.uber.marmaray.common.converters.schema.CassandraSchemaConverter;
import com.uber.marmaray.common.converters.schema.DataFrameSchemaConverter;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.job.JobDag;
import com.uber.marmaray.common.job.JobManager;
import com.uber.marmaray.common.metadata.CassandraBasedMetadataManager;
import com.uber.marmaray.common.metadata.IMetadataManager;
import com.uber.marmaray.common.metrics.DataFeedMetricNames;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.ErrorCauseTagNames;
import com.uber.marmaray.common.metrics.JobMetricNames;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.common.metrics.LongMetric;
import com.uber.marmaray.common.metrics.ModuleTagNames;
import com.uber.marmaray.common.metrics.TimerMetric;
import com.uber.marmaray.common.reporters.ConsoleReporter;
import com.uber.marmaray.common.reporters.Reporters;
import com.uber.marmaray.common.schema.cassandra.CassandraSchema;
import com.uber.marmaray.common.schema.cassandra.CassandraSinkSchemaManager;
import com.uber.marmaray.common.sinks.ISink;
import com.uber.marmaray.common.sinks.cassandra.CassandraClientSink;
import com.uber.marmaray.common.sinks.cassandra.CassandraSSTableSink;
import com.uber.marmaray.common.sources.ISource;
import com.uber.marmaray.common.sources.IWorkUnitCalculator;
import com.uber.marmaray.common.sources.hive.HiveSource;
import com.uber.marmaray.common.sources.hive.ParquetWorkUnitCalculator;
import com.uber.marmaray.common.spark.SparkArgs;
import com.uber.marmaray.common.spark.SparkFactory;
import com.uber.marmaray.utilities.SparkUtil;
import com.uber.marmaray.utilities.CassandraSinkUtil;
import com.uber.marmaray.utilities.ErrorExtractor;
import com.uber.marmaray.utilities.FSUtils;
import com.uber.marmaray.utilities.JobUtil;
import com.uber.marmaray.utilities.SchemaUtil;
import com.uber.marmaray.utilities.TimestampInfo;
import com.uber.marmaray.utilities.listener.TimeoutManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.hibernate.validator.constraints.NotEmpty;
import parquet.Preconditions;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Job to load data from parquet files on HDFS to a Cassandra instance
 */
@Slf4j
public class ParquetToCassandraJob {

    /**
     * Generic entry point
     * @param args arguments for the job, from the command line
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {
        new ParquetToCassandraJob().run(args);
    }

    /**
     * Main execution method for the job.
     * @param args command line arguments
     * @throws IOException
     */
    private void run(final String[] args) throws IOException {
        final Instant jobStartTime = Instant.now();

        final Configuration conf = getConfiguration(args);

        final Reporters reporters = new Reporters();
        reporters.addReporter(new ConsoleReporter());

        final Map<String, String> metricTags = Collections.emptyMap();
        final DataFeedMetrics dataFeedMetrics = new DataFeedMetrics("parquet to cassandra dispersal", metricTags);

        final FileSystem fs = FSUtils.getFs(conf, Optional.absent());

        log.info("Initializing configurations for job");
        final TimerMetric confInitMetric = new TimerMetric(DataFeedMetricNames.INIT_CONFIG_LATENCY_MS,
                metricTags);
        final HiveSourceConfiguration hiveSourceConf;
        final CassandraSinkConfiguration cassandraConf;
        try {
            hiveSourceConf = new HiveSourceConfiguration(conf);
            cassandraConf = new CassandraSinkConfiguration(conf, dataFeedMetrics);
        } catch (final Exception e) {
            final LongMetric configError = new LongMetric(DataFeedMetricNames.DISPERSAL_CONFIGURATION_INIT_ERRORS, 1);
            configError.addTags(metricTags);
            configError.addTags(DataFeedMetricNames
                    .getErrorModuleCauseTags(ModuleTagNames.CONFIGURATION, ErrorCauseTagNames.CONFIG_ERROR));
            reporters.report(configError);
            reporters.getReporters().forEach(reporter -> dataFeedMetrics.gauageFailureMetric(reporter));
            throw e;
        }
        confInitMetric.stop();
        reporters.report(confInitMetric);

        log.info("Initializing metadata manager for job");
        final TimerMetric metadataManagerInitMetric =
                new TimerMetric(DataFeedMetricNames.INIT_METADATAMANAGER_LATENCY_MS, metricTags);
        final IMetadataManager metadataManager;
        try {
            metadataManager = initMetadataManager(conf, dataFeedMetrics);
        } catch (final JobRuntimeException e) {
            final LongMetric configError = new LongMetric(DataFeedMetricNames.DISPERSAL_CONFIGURATION_INIT_ERRORS, 1);
            configError.addTags(metricTags);
            configError.addTags(DataFeedMetricNames
                    .getErrorModuleCauseTags(ModuleTagNames.METADATA_MANAGER, ErrorCauseTagNames.CONFIG_ERROR));
            reporters.report(configError);
            reporters.getReporters().forEach(reporter -> dataFeedMetrics.gauageFailureMetric(reporter));
            throw e;
        }
        metadataManagerInitMetric.stop();
        reporters.report(metadataManagerInitMetric);

        // Todo - T1021227: Consider using Schema Service instead
        log.info("Reading schema from path: {}", hiveSourceConf.getDataPath());
        final TimerMetric convertSchemaLatencyMs =
                new TimerMetric(DataFeedMetricNames.CONVERT_SCHEMA_LATENCY_MS, metricTags);
        final StructType inputSchema;
        try {
            inputSchema = SchemaUtil.generateSchemaFromParquet(fs,
                    hiveSourceConf.getDataPath(), Optional.of(dataFeedMetrics));
        } catch (final JobRuntimeException e) {
            final LongMetric configError = new LongMetric(DataFeedMetricNames.DISPERSAL_CONFIGURATION_INIT_ERRORS, 1);
            configError.addTags(metricTags);
            configError.addTags(DataFeedMetricNames
                    .getErrorModuleCauseTags(ModuleTagNames.SCHEMA_MANAGER, ErrorCauseTagNames.CONFIG_ERROR));
            reporters.report(configError);
            reporters.getReporters().forEach(reporter -> dataFeedMetrics.gauageFailureMetric(reporter));
            throw e;
        }

        final DataFrameSchemaConverter schemaConverter = new DataFrameSchemaConverter();
        final Schema outputSchema = schemaConverter.convertToCommonSchema(inputSchema);
        convertSchemaLatencyMs.stop();
        reporters.report(convertSchemaLatencyMs);

        final SparkArgs sparkArgs = new SparkArgs(
            Arrays.asList(outputSchema),
            SparkUtil.getSerializationClasses(),
            conf);
        final SparkFactory sparkFactory = new SparkFactory(sparkArgs);
        final JobManager jobManager = JobManager.createJobManager(conf, "marmaray",
                "frequency", sparkFactory, reporters);

        final JavaSparkContext jsc = sparkFactory.getSparkContext();
        try {
            log.info("Initializing converters & schemas for job");
            final SQLContext sqlContext = SQLContext.getOrCreate(jsc.sc());

            log.info("Common schema is: {}", outputSchema.toString());

            final TimestampInfo tsInfo = new TimestampInfo(cassandraConf.getWriteTimestamp(),
                    cassandraConf.isTimestampIsLongType(), cassandraConf.getTimestampFieldName());
            log.info("Using optional Cassandra timestamp: {}", tsInfo);

            final List<String> requiredKeys = Lists.newArrayList(
                Iterables.concat(
                    cassandraConf.getClusteringKeys()
                        .stream()
                        .map(ck -> ck.getName()).collect(Collectors.toList()),
                    cassandraConf.getPartitionKeys()));
            if (tsInfo.hasTimestamp()) {
                requiredKeys.remove(tsInfo.getTimestampFieldName());
            }
            log.info("Required keys for source and sink are: {}", requiredKeys);

            log.info("Initializing schema converter");
            final CassandraSchemaConverter cassandraSchemaConverter = new CassandraSchemaConverter(
                    cassandraConf.getKeyspace(), cassandraConf.getTableName(),
                    tsInfo, cassandraConf.getFilteredColumns());
            final CassandraSchema cassandraSchema = cassandraSchemaConverter.convertToExternalSchema(outputSchema);

            log.info("Initializing schema manager");
            final CassandraSinkSchemaManager schemaManager;
            try {
                schemaManager = new CassandraSinkSchemaManager(cassandraSchema,
                        cassandraConf.getPartitionKeys(),
                        cassandraConf.getClusteringKeys(),
                        cassandraConf.getTimeToLive(),
                        Optional.of(dataFeedMetrics),
                        CassandraSinkUtil.computeTimestamp(conf.getProperty(HiveConfiguration.PARTITION)),
                        cassandraConf.getWrittenTime().isPresent());
            } catch (JobRuntimeException e) {
                reporters.getReporters().forEach(reporter -> dataFeedMetrics.gauageFailureMetric(reporter));
                throw e;
            }

            log.info("Initializing source data converter");
            // Source
            final SparkSourceDataConverter dataConverter = new SparkSourceDataConverter(
                inputSchema,
                outputSchema,
                conf,
                Sets.newHashSet(requiredKeys),
                new ErrorExtractor());

            log.info("Initializing source & sink for job");
            final ISource hiveSource = new HiveSource(hiveSourceConf, sqlContext, dataConverter);
            final ISink cassandraSink;

            // Sink
            if (cassandraConf.getUseClientSink()) {
                final CassandraSinkCQLDataConverter sinkCQLDataConverter = new CassandraSinkCQLDataConverter(
                        outputSchema,
                        conf,
                        cassandraConf.getFilteredColumns(),
                        requiredKeys,
                        tsInfo,
                        new ErrorExtractor());
                cassandraSink = new CassandraClientSink(sinkCQLDataConverter, schemaManager, cassandraConf);
            } else {
                final CassandraSinkDataConverter sinkDataConverter = new CassandraSinkDataConverter(
                        outputSchema,
                        conf,
                        cassandraConf.getFilteredColumns(),
                        cassandraConf.getWrittenTime(),
                        requiredKeys,
                        tsInfo,
                        new ErrorExtractor());
                cassandraSink = new CassandraSSTableSink(sinkDataConverter, schemaManager, cassandraConf);
            }

            log.info("Initializing work unit calculator for job");
            final IWorkUnitCalculator workUnitCalculator = new ParquetWorkUnitCalculator(hiveSourceConf, fs);

            log.info("Initializing job dag");
            final JobDag jobDag = new JobDag(hiveSource, cassandraSink, metadataManager, workUnitCalculator,
                    hiveSourceConf.getJobName(), hiveSourceConf.getJobName(), new JobMetrics("marmaray"),
                    dataFeedMetrics, reporters);

            /*
             * We need to have separate m3 reporters since the JobManager automatically calls close() on
             * the reporter upon completion of the executed job and it can't be used to report latency
             * and other config related metrics in this class.
             */
            jobManager.addJobDag(jobDag);

            log.info("Running dispersal job");
            try {
                jobManager.run();
                JobUtil.raiseExceptionIfStatusFailed(jobManager.getJobManagerStatus());
            } catch (final Throwable t) {
                /*
                 * TODO - Technically more than 1 error can occur in a job run, but this currently acts
                 * as more like a flag that we can alert on.  Also, Metrics API as currently constructed doesn't
                 * have an elegant way to increment this count which should be improved.
                 *
                 * TODO: T131675 - Also, we will modify JobManager to increment the error count with the datafeed name
                 * which currently isn't possible.  This is the most appropriate place to increment the metric.
                 * Once that is done we can remove this code block to report the metric.
                 */
                if (TimeoutManager.getInstance().getTimedOut()) {
                    final LongMetric runTimeError = new LongMetric(DataFeedMetricNames.MARMARAY_JOB_ERROR, 1);
                    runTimeError.addTags(metricTags);
                    runTimeError.addTags(DataFeedMetricNames.getErrorModuleCauseTags(
                            ModuleTagNames.JOB_MANAGER, ErrorCauseTagNames.TIME_OUT));
                    reporters.report(runTimeError);
                }
                final LongMetric configError = new LongMetric(JobMetricNames.RUN_JOB_ERROR_COUNT, 1);
                configError.addTags(metricTags);
                reporters.report(configError);
                throw t;
            }
            log.info("Dispersal job has been completed");

            final TimerMetric jobLatencyMetric =
                    new TimerMetric(JobMetricNames.RUN_JOB_DAG_LATENCY_MS, metricTags, jobStartTime);
            jobLatencyMetric.stop();
            reporters.report(jobLatencyMetric);
            reporters.finish();
        } finally {
            jsc.stop();
        }
    }

    /**
     * Get configuration from command line
     * @param args command line arguments passed in
     * @return configuration populated from them
     */
    private Configuration getConfiguration(@NotEmpty final String[] args) {
        final ParquetToCassandraCommandLineOptions options = new ParquetToCassandraCommandLineOptions(args);
        if (options.getConfFile() != null) {
            return getFileConfiguration(options.getConfFile());
        } else if (options.getJsonConf() != null) {
            return getJsonConfiguration(options.getJsonConf());
        } else {
            throw new JobRuntimeException("Unable to find conf; this shouldn't be possible");
        }
    }

    /**
     * Get configuration from JSON-based configuration
     * @param jsonConf JSON string of configuration
     * @return configuration populated from it
     */
    private Configuration getJsonConfiguration(@NotEmpty final String jsonConf) {
        final Configuration conf = new Configuration();
        conf.loadYamlStream(IOUtils.toInputStream(jsonConf), Optional.absent());
        return conf;
    }

    /**
     * Load configuration from a file on HDFS
     * @param filePath path to the HDFS file to load
     * @return configuration populated from it
     */
    private Configuration getFileConfiguration(@NotEmpty final String filePath) {
        final Configuration conf = new Configuration();
        try {
            final FileSystem fs = FSUtils.getFs(conf, Optional.absent());
            final Path dataFeedConfFile = new Path(filePath);
            log.info("Loading configuration from {}", dataFeedConfFile.toString());
            conf.loadYamlStream(fs.open(dataFeedConfFile), Optional.absent());
        } catch (IOException e) {
            final String errorMessage = String.format("Unable to find configuration for %s", filePath);
            log.error(errorMessage);
            throw new JobRuntimeException(errorMessage, e);
        }
        return conf;

    }

    /**
     * Initialize the metadata store system
     * @param conf configuration to use
     * @param dataFeedMetric metric repository for reporting metrics
     * @return metadata mangaer
     */
    private static IMetadataManager initMetadataManager(@NonNull final Configuration conf,
                                                        @NonNull final DataFeedMetrics dataFeedMetric) {
        log.info("Create metadata manager");
        try {
            return new CassandraBasedMetadataManager(new CassandraMetadataManagerConfiguration(conf),
                    new AtomicBoolean(true));
        } catch (IOException e) {
            throw new JobRuntimeException("Unable to create metadata manager", e);
        }
    }

    private static final class ParquetToCassandraCommandLineOptions {
        @Getter
        @Parameter(names = {"--configurationFile", "-c"}, description = "path to configuration file")
        private String confFile;

        @Getter
        @Parameter(names = {"--jsonConfiguration", "-j"}, description = "json configuration")
        private String jsonConf;

        private ParquetToCassandraCommandLineOptions(@NonNull final String[] args) {
            final JCommander commander = new JCommander(this);
            commander.parse(args);
            Preconditions.checkState(this.confFile != null || this.jsonConf != null,
                    "One of jsonConfiguration or configurationFile must be specified");
        }
    }

}
