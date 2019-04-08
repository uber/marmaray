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
package com.uber.marmaray.common.metadata;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;

import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.uber.marmaray.common.configuration.CassandraMetadataManagerConfiguration;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.exceptions.MetadataException;
import com.uber.marmaray.common.metrics.DataFeedMetricNames;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.ErrorCauseTagNames;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.common.metrics.ModuleTagNames;
import com.uber.marmaray.common.schema.cassandra.CassandraMetadataSchemaManager;
import com.uber.marmaray.common.schema.cassandra.CassandraSchema;
import com.uber.marmaray.common.schema.cassandra.CassandraSchemaField;
import com.uber.marmaray.common.schema.cassandra.ClusterKey;
import com.uber.marmaray.utilities.MapUtil;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.Strings;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
/**
 * {@link CassandraBasedMetadataManager} implements the {@link IMetadataManager} interface.
 * It is a cassandra based metadata manager for HDFS files.
 */

public class CassandraBasedMetadataManager implements IMetadataManager<StringValue> {

    private static final List<CassandraSchemaField> fields = Collections.unmodifiableList(Arrays.asList(
            new CassandraSchemaField("job", CassandraSchemaField.STRING_TYPE),
            new CassandraSchemaField("time_stamp", CassandraSchemaField.STRING_TYPE),
            new CassandraSchemaField("checkpoint", CassandraSchemaField.STRING_TYPE)));

    private static final int maxTimestampCount = 5;

    protected final CassandraSchema schema;
    protected final CassandraMetadataSchemaManager schemaManager;
    private Optional<Map<String, StringValue>> metadataMap = Optional.absent();
    private int numTimestamps;
    private Optional<String> oldestTimestamp;

    @Getter
    private final AtomicBoolean shouldSaveChanges;

    @Getter
    private final String job;

    @Getter
    private final CassandraMetadataManagerConfiguration config;

    @Getter
    private Optional<DataFeedMetrics> dataFeedMetrics = Optional.absent();

    /**
     * initialize metadata manager table
     */
    public CassandraBasedMetadataManager(
            @NonNull final CassandraMetadataManagerConfiguration cassandraMetadataManagerConfig,
            @NonNull final AtomicBoolean shouldSaveChanges) throws IOException {
        this.config = cassandraMetadataManagerConfig;
        this.job = this.config.getJobName();

        this.shouldSaveChanges = shouldSaveChanges;
        this.numTimestamps = 0;
        this.oldestTimestamp = Optional.absent();

        this.schema = new CassandraSchema(this.config.getKeyspace(), this.config.getTableName(), this.fields);
        this.schemaManager = new CassandraMetadataSchemaManager(
                this.schema,
                Arrays.asList("job"),
                Arrays.asList(new ClusterKey("time_stamp", ClusterKey.Order.DESC)),
                Optional.absent());
    }

    private Map<String, StringValue> getMetadataMap() {
        if (!this.metadataMap.isPresent()) {
            try {
                this.metadataMap = Optional.of(generateMetaDataMap());
            } catch (IOException e) {
                log.error("Failed in loading cassandra based metadata manager", e);
                throw new JobRuntimeException(e);
            }
        }
        return this.metadataMap.get();
    }

    @Override
    public void setDataFeedMetrics(@NonNull final DataFeedMetrics dataFeedMetrics) {
        this.dataFeedMetrics = Optional.of(dataFeedMetrics);
    }

    @Override
    public void setJobMetrics(@NonNull final JobMetrics jobMetrics) {
        // ignored
    }

    /**
     * set operation applies on generated temporarily
     * map from Cassandra queries
     * @param key
     * @param value
     * @throws MetadataException
     */
    @Override
    public void set(@NotEmpty final String key, @NonNull final StringValue value) throws MetadataException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key));
        getMetadataMap().put(key, value);
    }

    /**
     * remove operation on map
     * @param key
     * @return
     */
    @Override
    public Optional<StringValue> remove(@NotEmpty final String key) {
        return Optional.fromNullable(getMetadataMap().remove(key));
    }

    /**
     * get operation from map
     * @param key
     * @return
     * @throws MetadataException
     */
    @Override
    public Optional<StringValue> get(@NotEmpty final String key) throws MetadataException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key));
        return getMetadataMap().containsKey(key) ? Optional.of(getMetadataMap().get(key)) : Optional.absent();
    }

    /**
     * @return
     * Returns all keys
     */
    @Override
    public Set<String> getAllKeys() {
        return getMetadataMap().keySet();
    }

    /**
     * Upon a successful job, this method keeps the latest checkpoint
     * and drops the oldest checkpoint if there are more than maxTimestampCount for
     * the job.
     *
     * @return
     * @throws IOException
     */
    @Override
    public void saveChanges() {
        if (this.shouldSaveChanges.compareAndSet(true, false)) {
            log.info("Saving checkpoint information to Cassandra");
        } else {
            log.info("Checkpoint info is already saved. Not saving it again.");
            return;
        }

        log.info("Connecting to Cassandara cluster");
        try (final Cluster cluster = getClusterBuilder().build();
            final Session session = cluster.connect()) {

            /**
             * oldest checkpoint is present only if maxTimestampCount checkpoints
             * are already present for current job
             */
            if (this.oldestTimestamp.isPresent()) {
                //delete older checkpoint before inserting the new one
                final String deleteCmd = this.schemaManager.generateDeleteOldestCheckpoint(
                        this.job, this.oldestTimestamp);
                log.info("Removing the oldest checkpoint with command: {}", deleteCmd);
                cmdExec(session, deleteCmd);
            }

            final Long currentTime = System.currentTimeMillis();
            final String checkpoint = serializeMetadataMap();
            String cassandraCols = String.format("job, time_stamp, checkpoint");
            String cassandraVals = String.format("'%s', '%s', '%s'",
                    this.job, currentTime.toString(), checkpoint);

            final String insertCmd = this.schemaManager.generateInsertStmt(cassandraCols, cassandraVals);
            log.info("Inserting {} to cassandra with command: {}", cassandraVals, insertCmd);
            cmdExec(session, insertCmd);
            log.info("Write back {} line", getMetadataMap().size());
        }
    }

    public Map<String, StringValue> generateMetaDataMap() throws IOException {
        log.info("Attempting to generate metadata map from Cassandra queries");

        log.info("Connecting to Cassandara cluster");
        try (final Cluster cluster = getClusterBuilder().build();
             final Session session = cluster.connect()) {

            final String setKeySpaceCmd = "USE marmaray;";
            log.info("setting keyspace with Cassandra command: {}", setKeySpaceCmd);
            cmdExec(session, setKeySpaceCmd);
            // check if table exists
            ResultSet results;
            log.info("Attempting to getting column names");

            try {
                final String columnNameQuery = this.schemaManager.getColumnsFromTableQuery();
                log.info("Getting column names with table query: {}", columnNameQuery);
                results = cmdExec(session, columnNameQuery);
            } catch (InvalidQueryException e) {
                if (this.dataFeedMetrics.isPresent()) {
                    this.dataFeedMetrics.get()
                            .createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_ERROR, 1,
                                    DataFeedMetricNames.getErrorModuleCauseTags(ModuleTagNames.METADATA_MANAGER,
                                            ErrorCauseTagNames.CASSANDRA_QUERY));
                }
                final String columnNameFromCFQuery = this.schemaManager.getColumnsFromColumnFamilyQuery();
                log.error("Saw an InvalidQueryException. Getting column names using column families: {}",
                        columnNameFromCFQuery);
                results = cmdExec(session, columnNameFromCFQuery);
            }

            final Map<String, String> columns = results.all()
                    .stream()
                    .collect(Collectors.toMap(r -> r.getString("column_name"), r ->  r.getString("type")));

            log.info("Found columns: {}", columns.toString());
            if (columns.isEmpty()) {
                final String createTable = this.schemaManager.generateCreateTableStmt();
                //TODO : do we consider not existing table as an error?
                log.info("No existing columns found. Creating table with Cassandra command: {}", createTable);
                cmdExec(session, this.schemaManager.generateCreateTableStmt());
                log.info("Returning empty metadata map");
                return new HashMap<String, StringValue>();
            } else {
                // this created new columns based on local specified schema
                // as of now we dont not expect this part to get exercised
                log.info("Generating alter table statements for any columns not found");
                this.schemaManager.generateAlterTableStmt(columns).forEach(stmt -> {
                        log.info("Altering Cassandra table with command: {}", stmt);
                        cmdExec(session, stmt);
                    });

                final String jobQuery = this.schemaManager.generateSelectJob(this.job, maxTimestampCount);
                log.info("Creating metadata map from Cassandra query: {}", jobQuery);
                ResultSet checkpoints = cmdExec(session, jobQuery);

                final List<Row> rows = checkpoints.all();
                if (rows.isEmpty()) {
                    log.info("No result was found, returning empty metadata map");
                    return new HashMap<String, StringValue>();
                } else {
                    this.numTimestamps = rows.size();
                    log.info("Number of founded checkpoints for job {} is: {}", this.job, this.numTimestamps);
                    if (this.numTimestamps == maxTimestampCount) {
                        this.oldestTimestamp = Optional.of((String) rows.get(rows.size() - 1).getObject("time_stamp"));
                        log.info("Keeping oldest checkpoint {}", this.oldestTimestamp.get());
                    } else if (this.numTimestamps > maxTimestampCount) {
                        if (this.dataFeedMetrics.isPresent()) {
                            this.dataFeedMetrics.get()
                                    .createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_ERROR, 1,
                                            DataFeedMetricNames.getErrorModuleCauseTags(ModuleTagNames.METADATA_MANAGER,
                                                    ErrorCauseTagNames.ERROR));
                        }
                    }
                    HashMap<String, StringValue> metadata = new HashMap<String, StringValue>();
                    final Row row = rows.get(0);
                    metadata = deserializeMetadata((String) row.getObject("checkpoint"));
                    log.info("Returning checkpoint");
                    return metadata;
                }
            }
        }
    }

    public void deleteAllMetadataOfJob(@NonNull final String jobName) {
        try (final Cluster cluster = getClusterBuilder().build();
        final Session session = cluster.connect()) {
            final String deleteCmd = this.schemaManager.generateDeleteJob(jobName);
            log.info("Deleteing all the checkpoints of job: {} with command: {}", jobName, deleteCmd);
            cmdExec(session, deleteCmd);
        }
    }

    private Cluster.Builder getClusterBuilder() {
        final Cluster.Builder builder = Cluster.builder().withClusterName(this.config.getCluster());
        builder.withCredentials(this.config.getUsername(), this.config.getPassword());
        this.config.getInitialHosts().stream().forEach(host -> builder.addContactPoint(host));
        if (this.config.getNativePort().isPresent()) {
            builder.withPort(Integer.parseInt(this.config.getNativePort().get()));
        } else {
            builder.withPort(Integer.parseInt(CassandraMetadataManagerConfiguration.DEFAULT_OUTPUT_NATIVE_PORT));
        }
        builder.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM));
        return builder;
    }

    private String serializeMetadataMap() {
        HashMap<String, String> metadata = new HashMap<String, String>();
        for (final Map.Entry<String, StringValue> entry : getMetadataMap().entrySet()) {
            metadata.put(entry.getKey(), entry.getValue().getValue());
        }
        String checkpoints = MapUtil.serializeMap(metadata);
        return checkpoints;
    }

    private HashMap<String, StringValue> deserializeMetadata(@NotEmpty final String rawMetadata) {
        HashMap<String, StringValue> metadata = new HashMap<String, StringValue>();
        Map<String, String> meta = MapUtil.deserializeMap(rawMetadata);
        for (final Map.Entry<String, String> entry : meta.entrySet()) {
            log.info("deserialize to metadata map ({} : {})", entry.getKey(), entry.getValue());
            metadata.put(entry.getKey(), new StringValue(entry.getValue()));
        }
        return metadata;
    }

    private ResultSet cmdExec(@NotEmpty final Session session, @NotEmpty final String command) {
        ResultSet result;
        try {
            result = session.execute(command);
        } catch (Exception e) {
            if (this.dataFeedMetrics.isPresent()) {
                this.dataFeedMetrics.get()
                        .createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_ERROR, 1,
                                DataFeedMetricNames.getErrorModuleCauseTags(ModuleTagNames.METADATA_MANAGER,
                                        ErrorCauseTagNames.EXEC_CASSANDRA_CMD));
            }
            log.error("Exception: {}", e);
            throw new JobRuntimeException(e);
        }
        return result;
    }
}
