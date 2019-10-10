package com.uber.marmaray.examples.job;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Optional;
import com.uber.marmaray.common.configuration.*;
import com.uber.marmaray.common.converters.data.*;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.job.JobDag;
import com.uber.marmaray.common.job.JobManager;
import com.uber.marmaray.common.metadata.HoodieBasedMetadataManager;
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
import com.uber.marmaray.common.schema.kafka.KafkaSchemaJSONServiceReader;
import com.uber.marmaray.common.sinks.hoodie.HoodieSink;
import com.uber.marmaray.common.sources.ISource;
import com.uber.marmaray.common.sources.IWorkUnitCalculator;
import com.uber.marmaray.common.sources.kafka.KafkaSource;
import com.uber.marmaray.common.sources.kafka.KafkaWorkUnitCalculator;
import com.uber.marmaray.common.spark.SparkArgs;
import com.uber.marmaray.common.spark.SparkFactory;
import com.uber.marmaray.utilities.SparkUtil;
import com.uber.marmaray.utilities.ErrorExtractor;
import com.uber.marmaray.utilities.FSUtils;
import com.uber.marmaray.utilities.JobUtil;
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
import org.hibernate.validator.constraints.NotEmpty;
import parquet.Preconditions;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.converters.data.HoodieSinkDataConverter;


/**
 * Job to load data from kafka to hoodie
 */
@Slf4j
public class KafkaToHoodieJob {

    /**
     * Generic entry point
     *
     * @param args arguments for the job, from the command line
     * @throws IOException Exception
     */
    public static void main(final String[] args) throws IOException {
        new KafkaToHoodieJob().run(args);
    }

    /**
     * Main execution method for the job.
     *
     * @param args command line arguments
     * @throws IOException Exception
     */
    private void run(final String[] args) throws IOException {

        final Instant jobStartTime = Instant.now();

        final Configuration conf = getConfiguration(args);

        final Reporters reporters = new Reporters();
        reporters.addReporter(new ConsoleReporter());

        final Map<String, String> metricTags = Collections.emptyMap();
        final DataFeedMetrics dataFeedMetrics = new DataFeedMetrics("KafkaToHoodieJob", metricTags);

        log.info("Initializing configurations for job");
        final TimerMetric confInitMetric = new TimerMetric(DataFeedMetricNames.INIT_CONFIG_LATENCY_MS,
                metricTags);

        final KafkaSourceConfiguration kafkaSourceConf;
        final HoodieConfiguration hoodieConf;
        try {
            kafkaSourceConf = new KafkaSourceConfiguration(conf);
            hoodieConf = new HoodieConfiguration(conf, "test_hoodie");
        } catch (final Exception e) {
            final LongMetric configError = new LongMetric(DataFeedMetricNames.DISPERSAL_CONFIGURATION_INIT_ERRORS, 1);
            configError.addTags(metricTags);
            configError.addTags(DataFeedMetricNames
                    .getErrorModuleCauseTags(ModuleTagNames.CONFIGURATION, ErrorCauseTagNames.CONFIG_ERROR));
            reporters.report(configError);
            reporters.getReporters().forEach(dataFeedMetrics::gauageFailureMetric);
            throw e;
        }
        confInitMetric.stop();
        reporters.report(confInitMetric);

        log.info("Reading schema");
        final TimerMetric convertSchemaLatencyMs =
                new TimerMetric(DataFeedMetricNames.CONVERT_SCHEMA_LATENCY_MS, metricTags);

        final Schema outputSchema = new Schema.Parser().parse(hoodieConf.getHoodieWriteConfig().getSchema());
        convertSchemaLatencyMs.stop();
        reporters.report(convertSchemaLatencyMs);

        final SparkArgs sparkArgs = new SparkArgs(
                Collections.singletonList(outputSchema),
                SparkUtil.getSerializationClasses(),
                conf);
        final SparkFactory sparkFactory = new SparkFactory(sparkArgs);
        final JobManager jobManager = JobManager.createJobManager(conf, "marmaray",
                "frequency", sparkFactory, reporters);

        final JavaSparkContext jsc = sparkFactory.getSparkContext();

        log.info("Initializing metadata manager for job");
        final TimerMetric metadataManagerInitMetric =
                new TimerMetric(DataFeedMetricNames.INIT_METADATAMANAGER_LATENCY_MS, metricTags);
        final IMetadataManager metadataManager;
        try {
            metadataManager = initMetadataManager(hoodieConf, jsc);
        } catch (final JobRuntimeException e) {
            final LongMetric configError = new LongMetric(DataFeedMetricNames.DISPERSAL_CONFIGURATION_INIT_ERRORS, 1);
            configError.addTags(metricTags);
            configError.addTags(DataFeedMetricNames
                    .getErrorModuleCauseTags(ModuleTagNames.METADATA_MANAGER, ErrorCauseTagNames.CONFIG_ERROR));
            reporters.report(configError);
            reporters.getReporters().forEach(dataFeedMetrics::gauageFailureMetric);
            throw e;
        }
        metadataManagerInitMetric.stop();
        reporters.report(metadataManagerInitMetric);

        try {
            log.info("Initializing converters & schemas for job");
            final SQLContext sqlContext = SQLContext.getOrCreate(jsc.sc());

            log.info("Common schema is: {}", outputSchema.toString());

            // Schema
            log.info("Initializing source data converter");
            KafkaSchemaJSONServiceReader serviceReader = new KafkaSchemaJSONServiceReader(outputSchema);
            final KafkaSourceDataConverter dataConverter = new KafkaSourceDataConverter(serviceReader, conf,
                    new ErrorExtractor());

            log.info("Initializing source & sink for job");
            final ISource kafkaSource = new KafkaSource(kafkaSourceConf, Optional.of(jsc), dataConverter,
                    Optional.absent(), Optional.absent());

            // Sink
            HoodieSinkDataConverter hoodieSinkDataConverter = new HoodieSinkDataConverter(conf, new ErrorExtractor(),
                    hoodieConf);
            HoodieSink hoodieSink = new HoodieSink(hoodieConf, hoodieSinkDataConverter, jsc, metadataManager,
                    Optional.absent());

            log.info("Initializing work unit calculator for job");
            final IWorkUnitCalculator workUnitCalculator = new KafkaWorkUnitCalculator(kafkaSourceConf);

            log.info("Initializing job dag");
            final JobDag jobDag = new JobDag(kafkaSource, hoodieSink, metadataManager, workUnitCalculator,
                    "test", "test", new JobMetrics("marmaray"), dataFeedMetrics,
                    reporters);

            jobManager.addJobDag(jobDag);

            log.info("Running dispersal job");
            try {
                jobManager.run();
                JobUtil.raiseExceptionIfStatusFailed(jobManager.getJobManagerStatus());
            } catch (final Throwable t) {
                if (TimeoutManager.getTimedOut()) {
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
     *
     * @param args command line arguments passed in
     * @return configuration populated from them
     */
    private Configuration getConfiguration(@NotEmpty final String[] args) {
        final KafkaToHoodieCommandLineOptions options = new KafkaToHoodieCommandLineOptions(args);
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
     *
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
     *
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
     *
     * @param conf configuration to use
     * @param jsc  Java spark context
     * @return metadata manager
     */
    private static IMetadataManager initMetadataManager(@NonNull final HoodieConfiguration conf,
                                                        @NonNull final JavaSparkContext jsc) {
        log.info("Create metadata manager");
        try {
            return new HoodieBasedMetadataManager(conf, new AtomicBoolean(true), jsc);
        } catch (IOException e) {
            throw new JobRuntimeException("Unable to create metadata manager", e);
        }
    }

    private static final class KafkaToHoodieCommandLineOptions {
        @Getter
        @Parameter(names = {"--configurationFile", "-c"}, description = "path to configuration file")
        private String confFile;

        @Getter
        @Parameter(names = {"--jsonConfiguration", "-j"}, description = "json configuration")
        private String jsonConf;

        private KafkaToHoodieCommandLineOptions(@NonNull final String[] args) {
            final JCommander commander = new JCommander(this);
            commander.parse(args);
            Preconditions.checkState(this.confFile != null || this.jsonConf != null,
                    "One of jsonConfiguration or configurationFile must be specified");
        }
    }

}
