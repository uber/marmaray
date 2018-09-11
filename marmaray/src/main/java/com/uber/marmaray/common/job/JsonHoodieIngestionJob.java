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

package com.uber.marmaray.common.job;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Optional;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.FileSourceConfiguration;
import com.uber.marmaray.common.configuration.HoodieConfiguration;
import com.uber.marmaray.common.converters.data.HoodieSinkDataConverter;
import com.uber.marmaray.common.converters.data.TSBasedHoodieSinkDataConverter;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.metadata.HoodieBasedMetadataManager;
import com.uber.marmaray.common.metadata.IMetadataManager;
import com.uber.marmaray.common.schema.HDFSSchemaService;
import com.uber.marmaray.common.schema.ISchemaService;
import com.uber.marmaray.common.sinks.hoodie.HoodieSink;
import com.uber.marmaray.common.sources.file.FileSource;
import com.uber.marmaray.common.sources.file.FileSourceDataConverter;
import com.uber.marmaray.common.sources.file.FileWorkUnitCalculator;
import com.uber.marmaray.common.sources.file.JSONFileSourceDataConverter;
import com.uber.marmaray.utilities.FSUtils;
import com.uber.marmaray.utilities.JsonSourceConverterErrorExtractor;
import com.uber.marmaray.utilities.StringTypes;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.uber.marmaray.utilities.ErrorTableUtil.ERROR_SCHEMA_IDENTIFIER;
import static com.uber.marmaray.utilities.ErrorTableUtil.ERROR_TABLE_KEY;
import static com.uber.marmaray.utilities.ErrorTableUtil.TABLE_KEY;
import static com.uber.marmaray.utilities.SparkUtil.SPARK_PROPERTIES_KEY_PREFIX;

/**
 * End to end job to load data from JSON files on HDFS to a hoodie table. Example of combining
 * {@link com.uber.marmaray.common.sources.ISource}, {@link com.uber.marmaray.common.sinks.ISink},
 * into a {@link JobDag} that can be run to load data.
 */
@Slf4j
public class JsonHoodieIngestionJob {

    public static final String APP_NAME = "MarmarayJsonHoodieIngestion";
    public static final String ONLY_TABLE = "only_table";
    public static final String ERROR_TABLE = "error_table";
    public static final String ERROR_SCHEMA_NAME = "errorSchema";

    public static void main(final String[] args) throws Exception {
        final JsonHoodieIngestionCommandLineOptions cmd = new JsonHoodieIngestionCommandLineOptions(args);

        final Configuration conf = getConfiguration(cmd);

        final JobManager jobManager = JobManager.createJobManager(conf, APP_NAME, StringTypes.EMPTY, false);

        final ISchemaService schemaService = new HDFSSchemaService(conf);
        final String schemaName = conf.getProperty(FileSourceConfiguration.SCHEMA).get();
        final Schema schema = schemaService.getSchema(schemaName);
        jobManager.addSchema(schema);
        final Schema errorSchema = schemaService.getSchema(ERROR_SCHEMA_NAME);

        jobManager.getConf().setProperty(SPARK_PROPERTIES_KEY_PREFIX + ERROR_SCHEMA_IDENTIFIER, errorSchema.toString());
        jobManager.getConf().setProperty(SPARK_PROPERTIES_KEY_PREFIX + TABLE_KEY, ONLY_TABLE);
        jobManager.getConf().setProperty(SPARK_PROPERTIES_KEY_PREFIX + ERROR_TABLE_KEY, ERROR_TABLE);
        final JavaSparkContext jsc = jobManager.getOrCreateSparkContext();

        final FileSourceDataConverter sourceDataConverter =
            new JSONFileSourceDataConverter(conf, new JsonSourceConverterErrorExtractor(), schema);
        final FileSource fileSource = new FileSource(new FileSourceConfiguration(conf), jsc, sourceDataConverter);
        final HoodieSinkDataConverter sinkDataConverter =
            new TSBasedHoodieSinkDataConverter(conf, "firstName", "timestamp", TimeUnit.SECONDS);
        final HoodieConfiguration hoodieConf = HoodieConfiguration.newBuilder(conf, ONLY_TABLE)
            .withSchema(schema.toString())
            .build();
        final IMetadataManager metadataMgr =
            new HoodieBasedMetadataManager(hoodieConf, new AtomicBoolean(true), jsc);
        final HoodieSink hoodieSink = new HoodieSink(
            hoodieConf,
            sinkDataConverter,
            jsc,
            HoodieSink.HoodieSinkOp.INSERT,
            metadataMgr);
        final FileWorkUnitCalculator workUnitCalculator = new FileWorkUnitCalculator(new FileSourceConfiguration(conf));

        final JobDag jobDag = new JobDag(
            fileSource,
            hoodieSink,
            metadataMgr,
            workUnitCalculator,
            APP_NAME,
            schemaName,
            jobManager.getJobMetrics(),
            jobManager.getReporters());

        jobManager.addJobDag(jobDag);
        jobManager.run();
    }

    private static Configuration getConfiguration(@NonNull final JsonHoodieIngestionCommandLineOptions cmd) {
        final Configuration conf = new Configuration();
        try {
            final FileSystem fs = FSUtils.getFs(conf);
            final Path dataFeedConfFile = new Path(cmd.getConfFile());
            log.info("Loading configuration from {}", dataFeedConfFile.toString());
            conf.loadYamlStream(fs.open(dataFeedConfFile), Optional.absent());
        } catch (IOException e) {
            final String errorMessage = String.format("Unable to find configuration for %s", cmd.getConfFile());
            log.error(errorMessage);
            throw new JobRuntimeException(errorMessage, e);
        }
        return conf;
    }

    private static final class JsonHoodieIngestionCommandLineOptions {
        @Getter
        @Parameter(names = {"--configurationFile", "-c"}, description = "path to configuration file", required = true)
        private String confFile;

        private JsonHoodieIngestionCommandLineOptions(@NonNull final String[] args) {
            final JCommander commander = new JCommander(this);
            commander.parse(args);
        }
    }

}
