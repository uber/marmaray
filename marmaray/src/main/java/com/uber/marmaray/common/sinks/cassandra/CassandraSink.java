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
package com.uber.marmaray.common.sinks.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.uber.marmaray.common.configuration.CassandraSinkConfiguration;
import com.uber.marmaray.common.metrics.DataFeedMetricNames;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.ErrorCauseTagNames;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.common.metrics.ModuleTagNames;
import com.uber.marmaray.common.schema.cassandra.CassandraSinkSchemaManager;
import com.uber.marmaray.common.sinks.ISink;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlBulkOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * {@link CassandraSink} implements the {@link ISink} interface for a Cassandra sink. The AvroPayload RDD
 * will be forked into valid & invalid records based on set criteria.
 *
 * Valid records will then be written to the Cassandra backend
 */
@Slf4j
public abstract class CassandraSink implements ISink, Serializable {
    protected final CassandraSinkConfiguration conf;
    protected final CassandraSinkSchemaManager schemaManager;
    protected transient Optional<DataFeedMetrics> tableMetrics = Optional.absent();

    public CassandraSink(@NonNull final CassandraSinkSchemaManager schemaManager,
                                @NonNull final CassandraSinkConfiguration conf) {
        this.schemaManager = schemaManager;
        this.conf = conf;
    }

    @Override
    public void setDataFeedMetrics(@NonNull final DataFeedMetrics dataFeedMetrics) {
        this.tableMetrics = Optional.of(dataFeedMetrics);
    }

    @Override
    public void setJobMetrics(@NonNull final JobMetrics jobMetrics) {
        // ignored
    }

    /**
     * This method prepares the Cassandra table for the bulk load
     *
     * @param hadoopConf
     */
    void setupCassandraTable(@NonNull final Configuration hadoopConf) {
        final Cluster.Builder builder = getBuilder(hadoopConf);

        final String keySpace = this.conf.getKeyspace();

        log.info("Connecting cluster with keyspace : {}", keySpace);

        try (final Cluster cluster = builder.build();
             final Session session = cluster.connect(keySpace)) {
            ResultSet results;

            log.info("Attempting to getting column names");
            try {
                final String columnNameQuery = this.schemaManager.getColumnsFromTableQuery();
                log.info("Getting column names with table query: {}", columnNameQuery);
                results = cmdExec(session, columnNameQuery);
            } catch (InvalidQueryException e) {
                final String columnNameFromCFQuery = this.schemaManager.getColumnsFromColumnFamilyQuery();
                log.error("Saw an InvalidQueryException. Getting column names using column families: {}",
                        columnNameFromCFQuery);
                results = cmdExec(session, columnNameFromCFQuery);
            }

            final Map<String, String> columns = results.all()
                    .stream()
                    .collect(Collectors.toMap(r -> r.getString("column_name"), r ->  r.getString("type")));

            if (columns.isEmpty()) {
                log.info("No existing columns found.  Executing create table statement: {}",
                        this.schemaManager.generateCreateTableStmt());
                cmdExec(session, this.schemaManager.generateCreateTableStmt());
                log.info("Create table statement executed");
            } else {
                log.info("Generating alter table statements for any columns not found");
                this.schemaManager.generateAlterTableStmt(columns).forEach(stmt -> {
                        log.info("Executing statement: {}", stmt);
                        cmdExec(session, stmt);
                    });
            }
        }

        log.info("Using keyspace {}", this.conf.getKeyspace());
        log.info("Using table name: {}", this.conf.getTableName());
        ConfigHelper.setOutputColumnFamily(hadoopConf,
                this.conf.getKeyspace(),
                this.conf.getTableName());

        log.info("Using table schema: {}", this.schemaManager.generateCreateTableStmt());
        CqlBulkOutputFormat.setTableSchema(hadoopConf,
                this.conf.getTableName(),
                this.schemaManager.generateCreateTableStmt());

        log.info("Using insert statement: {}", this.schemaManager.generateInsertStmt());
        CqlBulkOutputFormat.setTableInsertStatement(hadoopConf,
                this.conf.getTableName(),
                this.schemaManager.generateInsertStmt());
    }

    protected Cluster.Builder getBuilder(@NonNull final Configuration hadoopConf) {
        return getBuilder(hadoopConf, this.conf.getInitialHosts());
    }

    @VisibleForTesting
    public Cluster.Builder getBuilder(@NonNull final Configuration hadoopConf,
                                      @NonNull final List<String> initialHosts) {
        ConfigHelper.setOutputRpcPort(hadoopConf, CassandraSinkConfiguration.DEFAULT_OUTPUT_RPC_PORT);
        final Cluster.Builder builder = Cluster.builder().withClusterName(this.conf.getClusterName());
        if (!Strings.isNullOrEmpty(ConfigHelper.getOutputKeyspaceUserName(hadoopConf))
                && !Strings.isNullOrEmpty(ConfigHelper.getOutputKeyspacePassword(hadoopConf))) {
            builder.withCredentials(
                    ConfigHelper.getOutputKeyspaceUserName(hadoopConf),
                    ConfigHelper.getOutputKeyspacePassword(hadoopConf)
            );
        }
        initialHosts.forEach(builder::addContactPoint);

        if (this.conf.getNativePort().isPresent()) {
            builder.withPort(Integer.parseInt(this.conf.getNativePort().get()));
        } else {
            builder.withPort(Integer.parseInt(CassandraSinkConfiguration.DEFAULT_OUTPUT_NATIVE_PORT));
        }
        return builder;
    }

    protected ResultSet cmdExec(@NotEmpty final Session session, @NotEmpty final String command) {
        ResultSet result;
        try {
            result = session.execute(command);
        } catch (Exception e) {
            if (this.tableMetrics.isPresent()) {
                this.tableMetrics.get()
                        .createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_ERROR, 1, DataFeedMetricNames
                                .getErrorModuleCauseTags(ModuleTagNames.SINK, ErrorCauseTagNames.EXEC_CASSANDRA_CMD));
            }
            log.error("Error while running a cassandra command.");
            result = null;
        }
        return result;
    }
}
