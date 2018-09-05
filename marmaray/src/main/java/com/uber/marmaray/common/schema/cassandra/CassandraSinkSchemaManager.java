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
package com.uber.marmaray.common.schema.cassandra;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.uber.marmaray.common.schema.ISinkSchemaManager;
import com.uber.marmaray.utilities.StringTypes;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class CassandraSinkSchemaManager implements ISinkSchemaManager, Serializable {
    private static final Joiner joiner = Joiner.on(",").skipNulls();
    private static final String COLUMN_NAME = "column_name";
    private static final String SYSTEM_SCHEMA_COLS = "system_schema.columns";
    private static final String KEYSPACE_NAME = "keyspace_name";

    /**
     * todo - consider including partition & clustering keys in schema itself.  This isn't done here
     * because it gives us greater flexibility to have different partitioning/clustering schemes
     * based on a single set of fields in Cassandra Schema.  We also have validation code to ensure all
     * partitioning/clustering keys are actually a valid field.  Keeping this open to change in the future.
     */
    @Getter
    final CassandraSchema schema;
    final List<String> partitionKeys;
    final List<ClusterKey> clusteringKeys;
    final Optional<Long> ttl;

    // todo T928813: consider adding support for index creation

    public CassandraSinkSchemaManager(@NonNull final CassandraSchema schema,
                                      @NonNull final List<String> partitionKeys,
                                      @NonNull final List<ClusterKey> clusteringKeys,
                                      @NonNull final Optional<Long> ttl) {
        this.schema = schema;
        this.partitionKeys = partitionKeys;
        this.clusteringKeys = clusteringKeys;
        this.ttl = ttl;

        validateSchema();
    }

    public CassandraSinkSchemaManager(@NonNull final CassandraSchema schema,
                                      @NonNull final List<String> partitionKeys,
                                      @NonNull final List<ClusterKey> clusteringKeys) {
        this(schema, partitionKeys, clusteringKeys, Optional.absent());
    }

    public String generateCreateTableStmt() {
        final String fields = generateFieldsSyntax();
        final String primaryKeyStmt = generatePrimaryKeySyntax();
        final String clusteringOrder = generateClusteringOrderSyntax();
        final String createStatement = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s, %s) %s",
                this.schema.getKeySpace(),
                this.schema.getTableName(),
                fields,
                primaryKeyStmt,
                clusteringOrder);
        log.info("Generated table schema is {}", createStatement);
        return createStatement;
    }

    /**
     * The schema is the source of truth here and if any of the schema field names aren't found
     * in the existing columns we need to alter that table and update the schema
     *
     * @param existingColumns
     * @return List of strings, one for each column that needs to be added
     */
    public List<String> generateAlterTableStmt(final List<String> existingColumns) {
        final List<String> fieldNames = this.schema.getFields().stream()
                .map(field -> field.getFieldName()).collect(Collectors.toList());
        log.info("Existing field names in schema: {}", Arrays.toString(fieldNames.toArray()));
        final List<String> missingCols = fieldNames.stream()
                .filter(field -> !existingColumns.contains(field))
                .collect(Collectors.toList());
        log.info("Missing columns (if any): {}", Arrays.toString(missingCols.toArray()));
        return this.schema.getFields().stream()
                .filter(field -> missingCols.contains(field.getFieldName()))
                .map(field -> String.format("ALTER TABLE %s.%s ADD %s",
                        this.schema.getKeySpace(), this.schema.getTableName(), field.toString()))
                .collect(Collectors.toList());
    }

    /**
     * @return
     * Insert statement used for sstable loading using the fields and ttl value
     */
    public String generateInsertStmt() {
        final String fields = this.schema.getFields()
                .stream()
                .map(f -> StringTypes.SPACE + f.getFieldName())
                .collect(Collectors.joining(","));

        final String values = this.schema.getFields()
                .stream()
                .map(f -> "?")
                .collect(Collectors.joining(","));

        final String ttlStr = this.ttl.isPresent() ? "USING TTL " + this.ttl.get().toString() : StringTypes.EMPTY;

        return String.format("INSERT INTO %s.%s ( %s ) VALUES ( %s ) %s",
                this.schema.getKeySpace(),
                this.schema.getTableName(),
                fields,
                values,
                ttlStr);
    }

    /**
     * @return
     * Returns all the column names for a specific Cassandra table
     */
    public String getColumnNamesFromTableQuery() {
        return String.format("SELECT %s FROM %s WHERE %s = '%s' "
                + "AND table_name = '%s'",
                COLUMN_NAME,
                SYSTEM_SCHEMA_COLS,
                KEYSPACE_NAME,
                this.schema.getKeySpace(),
                this.schema.getTableName());
    }

    /**
     * @return
     * Returns all the column names for a specific column family
     */
    public String getColumnNamesFromColumnFamilyQuery() {
        return String.format("SELECT %s FROM %s WHERE %s = '%s' "
                + "AND columnfamily_name = '%s'",
                COLUMN_NAME,
                SYSTEM_SCHEMA_COLS,
                KEYSPACE_NAME,
                this.schema.getKeySpace(),
                this.schema.getTableName());
    }

    /**
     * @return
     * Returns a comma separated list of all the fields in the schema
     */
    private String generateFieldsSyntax() {
        final List<String> fields = this.schema.getFields().stream()
                .map(field -> field.toString()).collect(Collectors.toList());
        return joiner.join(fields);
    }

    /**
     * @return
     * Returns the syntax to define a primary key in a create table statement
     */
    private String generatePrimaryKeySyntax() {
        final List<String> clusterKeyNames =
                this.clusteringKeys.stream().map(key -> key.name).collect(Collectors.toList());
        return String.format("PRIMARY KEY ((%s)%s)", joiner.join(this.partitionKeys),
                clusterKeyNames.isEmpty() ? StringTypes.EMPTY : "," + joiner.join(clusterKeyNames));
    }

    /**
     * @return
     * Returns the optional clustering order syntax if it needs to be created.  The clustering key should
     * also define if it is in ascending (default) or descending order
     */
    private String generateClusteringOrderSyntax() {
        if (this.clusteringKeys.isEmpty()) {
            return StringTypes.EMPTY;
        }

        final List<String> clusterOrder =
                this.clusteringKeys.stream().map(key -> key.toString()).collect(Collectors.toList());

        return String.format("WITH CLUSTERING ORDER BY (%s)", joiner.join(clusterOrder));
    }

    /**
     * Validates all the private member variables and ensures that we can successfully create a cassandra table
     * If any of the conditions are not met we throw an exception
     * <p>
     * We assume that field names are case insensitive so we handle that accordingly
     *
     * @throws IllegalStateException
     */
    private void validateSchema() {
        Preconditions.checkState(!Strings.isNullOrEmpty(this.schema.getKeySpace()), "Keyspace is missing");
        Preconditions.checkState(!Strings.isNullOrEmpty(this.schema.getTableName()), "Table name is missing");
        Preconditions.checkState(this.schema.getFields() != null && !this.schema.getFields().isEmpty(),
                "Schema fields missing");
        Preconditions.checkState(this.partitionKeys != null && !this.partitionKeys.isEmpty(),
                "Partition key(s) missing");
        Preconditions.checkNotNull(this.clusteringKeys, "Clustering keys is null");

        final List<String> lowerCasePartitionKeys = this.partitionKeys
                .stream()
                .map(p -> p.toLowerCase())
                .collect(Collectors.toList());

        // Partition keys and clustering keys should always be completely independent lists
        final List<String> clusterKeyNames =
                this.clusteringKeys
                        .stream()
                        .map(key -> key.name.toLowerCase())
                        .collect(Collectors.toList());

        final List<String> duplicateKeys =
                lowerCasePartitionKeys
                        .stream()
                        .filter(p -> clusterKeyNames.contains(p.toLowerCase()))
                        .collect(Collectors.toList());
        Preconditions.checkState(duplicateKeys.isEmpty(), "Partition and clustering keys should have no overlap");

        // Each partition key should be found in fields
        final List<String> fieldNames =
                this.schema.getFields()
                        .stream()
                        .map(f -> f.getFieldName().toLowerCase())
                        .collect(Collectors.toList());

        log.info("Field names found: {}", fieldNames.size());
        fieldNames.stream().forEach(f -> log.info("Schema field: {}", f));

        Preconditions.checkState(fieldNames.containsAll(lowerCasePartitionKeys),
                "One or more of your partition keys were not found in the available schema fields");

        // Each clustering key should also be found in fields
        if (!clusteringKeys.isEmpty()) {
            Preconditions.checkState(fieldNames.containsAll(clusterKeyNames),
                    "Clustering keys not found in field names to disperse");
        }
    }
}
