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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.uber.marmaray.common.util.CassandraTestUtil;
import com.uber.marmaray.common.util.SchemaTestUtil;
import com.uber.marmaray.utilities.StringTypes;
import org.apache.avro.Schema;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.uber.marmaray.common.util.CassandraTestConstants.KEY_SPACE;
import static com.uber.marmaray.common.util.CassandraTestConstants.TABLE;


public class TestCassandraSinkSchemaManager {
    private static final String getColumnsQuery = String.format("SELECT column_name FROM system_schema.columns WHERE "
            + "keyspace_name = '%s' AND table_name = '%s'", KEY_SPACE, TABLE);

    private static final List<CassandraSchemaField> fields = Collections.unmodifiableList(Arrays.asList(
            new CassandraSchemaField("country_code", CassandraSchemaField.convertFromAvroType(
                    SchemaTestUtil.getSchema(CassandraSchemaField.STRING_TYPE))),
            new CassandraSchemaField("state_province", CassandraSchemaField.convertFromAvroType(
                    SchemaTestUtil.getSchema(CassandraSchemaField.STRING_TYPE))),
            new CassandraSchemaField("city", CassandraSchemaField.convertFromAvroType(
                    SchemaTestUtil.getSchema(CassandraSchemaField.STRING_TYPE))),
            new CassandraSchemaField("capacity", CassandraSchemaField.convertFromAvroType(
                    SchemaTestUtil.getSchema(CassandraSchemaField.INT_TYPE))),
            new CassandraSchemaField("gym_name", CassandraSchemaField.convertFromAvroType(
                    SchemaTestUtil.getSchema(CassandraSchemaField.STRING_TYPE)))));

    private static final List<CassandraSchemaField> newFields = Collections.unmodifiableList(Arrays.asList(
            new CassandraSchemaField("new_field1", CassandraSchemaField.convertFromAvroType(
                    SchemaTestUtil.getSchema(CassandraSchemaField.STRING_TYPE))),
            new CassandraSchemaField("new_field2", CassandraSchemaField.convertFromAvroType(
                    SchemaTestUtil.getSchema(CassandraSchemaField.STRING_TYPE)))));

    @Before
    public void setupTest() throws TTransportException, IOException {
        CassandraTestUtil.setupCluster();
    }

    @After
    public void teardownTest() {
        CassandraTestUtil.teardownCluster();
    }

    @Test
    public void testCreateTableWithSinglePrimaryKeyAndNoClusteringKey() {
        final CassandraSchema schema = new CassandraSchema(KEY_SPACE, TABLE, fields);
        final CassandraSinkSchemaManager schemaManager = new CassandraSinkSchemaManager(
                schema,
                Collections.singletonList("country_code"),
                Collections.EMPTY_LIST);
        final String createTableStmt = schemaManager.generateCreateTableStmt();
        final String expected = "CREATE TABLE IF NOT EXISTS marmaray.crossfit_gyms (country_code text,"
                + "state_province text,city text,capacity int,gym_name text, PRIMARY KEY ((country_code))) ";
        Assert.assertEquals(expected, createTableStmt);

        try (final Session session = getSession()) {
            session.execute(createTableStmt);
            validateCreateTable(session);
        }
    }

    @Test
    public void testCreateTableWithSinglePrimaryKeyAndOneClusteringKeyDesc() {
        final CassandraSchema schema = new CassandraSchema(KEY_SPACE, TABLE, fields);
        final CassandraSinkSchemaManager schemaManager = new CassandraSinkSchemaManager(
                schema,
                Collections.singletonList("country_code"),
                Collections.singletonList(new ClusterKey("state_province", ClusterKey.Order.DESC)));
        final String createTableStmt = schemaManager.generateCreateTableStmt();
        final String expected = "CREATE TABLE IF NOT EXISTS marmaray.crossfit_gyms (country_code text," +
                "state_province text,city text,capacity int,gym_name text, PRIMARY KEY ((country_code),state_province))"
                + " WITH CLUSTERING ORDER BY (state_province DESC)";
        Assert.assertEquals(expected, createTableStmt);

        try (final Session session = getSession()) {
            session.execute(createTableStmt);
            validateCreateTable(session);
        }
    }

    @Test
    public void testCreateTableWithMultiplePrimaryKeys() {
        final CassandraSchema schema = new CassandraSchema(KEY_SPACE, TABLE, fields);
        final CassandraSinkSchemaManager schemaManager = new CassandraSinkSchemaManager(
                schema,
                Arrays.asList("country_code", "state_province"),
                Collections.EMPTY_LIST);
        final String createTableStmt = schemaManager.generateCreateTableStmt();
        final String expected = "CREATE TABLE IF NOT EXISTS marmaray.crossfit_gyms (country_code text," +
                "state_province text,city text,capacity int,gym_name text, PRIMARY KEY ((country_code,state_province))) ";
        Assert.assertEquals(expected, createTableStmt);

        try (final Session session = getSession()) {
            session.execute(createTableStmt);
            validateCreateTable(session);
        }
    }

    @Test
    public void testCreateTableWithMultiplePrimaryKeysAndClusteringKeys() {
        final CassandraSchema schema = new CassandraSchema(KEY_SPACE, TABLE, fields);
        final CassandraSinkSchemaManager schemaManager = new CassandraSinkSchemaManager(
                schema,
                Arrays.asList("country_code", "state_province"),
                Arrays.asList(new ClusterKey("city", ClusterKey.Order.DESC),
                        new ClusterKey("gym_name", ClusterKey.Order.ASC)));
        final String createTableStmt = schemaManager.generateCreateTableStmt();
        final String expected = "CREATE TABLE IF NOT EXISTS marmaray.crossfit_gyms (country_code text," +
                "state_province text,city text,capacity int,gym_name text, PRIMARY KEY ((country_code,state_province)" +
                ",city,gym_name)) WITH CLUSTERING ORDER BY (city DESC,gym_name ASC)";
        Assert.assertEquals(expected, createTableStmt);

        try (final Session session = getSession()) {
            session.execute(createTableStmt);
            validateCreateTable(session);
        }
    }

    @Test
    public void testAlterTableWithOneNewColumn() {
        createBasicTable();

        final List<CassandraSchemaField> joinedList = Lists.newArrayList(Iterables.concat(fields,
                Collections.singletonList(new CassandraSchemaField("new_field1", "text"))));
        final CassandraSchema schema = new CassandraSchema(KEY_SPACE, TABLE, joinedList);
        final CassandraSinkSchemaManager schemaManager = new CassandraSinkSchemaManager(
                schema,
                Arrays.asList("country_code", "state_province"),
                Arrays.asList(new ClusterKey("city", ClusterKey.Order.DESC),
                        new ClusterKey("gym_name", ClusterKey.Order.ASC)));

        final List<String> alterTableQueries =
                schemaManager.generateAlterTableStmt(fields.stream().map(f -> f.getFieldName()).collect(Collectors.toList()));

        Assert.assertTrue(alterTableQueries.size() == 1);
        Assert.assertEquals("ALTER TABLE marmaray.crossfit_gyms ADD new_field1 text", alterTableQueries.get(0));
        try (final Session session = getSession()) {
            alterTableQueries.stream().forEach(query -> session.execute(query));
            validateAlterTable(session, Collections.singletonList("new_field1"));
        }
    }

    @Test
    public void testAlterTableWithOneMultipleColumns() {
        createBasicTable();

        final List<CassandraSchemaField> joinedList = Lists.newArrayList(Iterables.concat(fields, newFields));
        final CassandraSchema schema = new CassandraSchema(KEY_SPACE, TABLE, joinedList);
        final CassandraSinkSchemaManager schemaManager = new CassandraSinkSchemaManager(
                schema,
                Arrays.asList("country_code", "state_province"),
                Arrays.asList(new ClusterKey("city", ClusterKey.Order.DESC),
                        new ClusterKey("gym_name", ClusterKey.Order.ASC)));

        final List<String> alterTableQueries =
                schemaManager.generateAlterTableStmt(fields.stream().map(f -> f.getFieldName()).collect(Collectors.toList()));

        Assert.assertTrue(alterTableQueries.size() == 2);
        Assert.assertEquals("ALTER TABLE marmaray.crossfit_gyms ADD new_field1 text", alterTableQueries.get(0));
        Assert.assertEquals("ALTER TABLE marmaray.crossfit_gyms ADD new_field2 text", alterTableQueries.get(1));
        try (final Session session = getSession()) {
            alterTableQueries.stream().forEach(query -> session.execute(query));
            validateAlterTable(session, newFields.stream().map(f -> f.getFieldName()).collect(Collectors.toList()));
        }
    }

    @Test
    public void testGenerateColumnNameQueries() {
        final CassandraSchema schema = new CassandraSchema(KEY_SPACE, TABLE, fields);
        final CassandraSinkSchemaManager schemaManager = new CassandraSinkSchemaManager(
                schema,
                Arrays.asList("country_code", "state_province"),
                Arrays.asList(new ClusterKey("city", ClusterKey.Order.DESC),
                        new ClusterKey("gym_name", ClusterKey.Order.ASC)));
        final String cfQuery = schemaManager.getColumnNamesFromColumnFamilyQuery();
        final String tableQuery = schemaManager.getColumnNamesFromTableQuery();
        Assert.assertEquals("SELECT column_name FROM system_schema.columns WHERE "
                + "keyspace_name = 'marmaray' AND columnfamily_name = 'crossfit_gyms'", cfQuery);
        Assert.assertEquals("SELECT column_name FROM system_schema.columns WHERE "
                + "keyspace_name = 'marmaray' AND table_name = 'crossfit_gyms'", tableQuery);
    }

    @Test
    public void testInsertStatement() {
        final CassandraSchema schema = new CassandraSchema(KEY_SPACE, TABLE, fields);

        final CassandraSinkSchemaManager schemaManagerNoTTL = new CassandraSinkSchemaManager(
                schema,
                Arrays.asList("country_code", "state_province"),
                Collections.EMPTY_LIST,
                Optional.of(10000L));
        final String insertStmt = schemaManagerNoTTL.generateInsertStmt();
        final String expected = "INSERT INTO marmaray.crossfit_gyms (  country_code, state_province, city, "
                + "capacity, gym_name ) VALUES ( ?,?,?,?,? ) USING TTL 10000";
        Assert.assertEquals(expected, insertStmt);

        final CassandraSinkSchemaManager schemaManagerWithTTL = new CassandraSinkSchemaManager(
                schema,
                Arrays.asList("country_code", "state_province"),
                Collections.EMPTY_LIST,
                Optional.absent());
        final String insertStmtWithTTL = schemaManagerWithTTL.generateInsertStmt();
        Assert.assertEquals(expected.replace("USING TTL 10000", StringTypes.EMPTY), insertStmtWithTTL);
    }

    @Test(expected = IllegalStateException.class)
    public void testPartitionAndClusterKeyHaveSameName() {
        final CassandraSchema schema = new CassandraSchema(KEY_SPACE, TABLE, fields);
        final CassandraSinkSchemaManager schemaManager = new CassandraSinkSchemaManager(
                schema,
                Arrays.asList("country_code", "state_province"),
                Arrays.asList(new ClusterKey("country_code", ClusterKey.Order.DESC),
                        new ClusterKey("gym_name", ClusterKey.Order.ASC)));
        Assert.fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testEmptyKeySpace() {
        final CassandraSchema schema = new CassandraSchema(StringTypes.EMPTY, TABLE, fields);
        final CassandraSinkSchemaManager schemaManager = new CassandraSinkSchemaManager(
                schema,
                Arrays.asList("country_code", "state_province"),
                Arrays.asList(new ClusterKey("country_code", ClusterKey.Order.DESC),
                        new ClusterKey("gym_name", ClusterKey.Order.ASC)));
        Assert.fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testEmptyTableName() {
        final CassandraSchema schema = new CassandraSchema(KEY_SPACE, StringTypes.EMPTY, fields);
        final CassandraSinkSchemaManager schemaManager = new CassandraSinkSchemaManager(
                schema,
                Arrays.asList("country_code", "state_province"),
                Collections.EMPTY_LIST);
        Assert.fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testFieldsIsNull() {
        final CassandraSchema schema = new CassandraSchema(KEY_SPACE, TABLE, null);
        final CassandraSinkSchemaManager schemaManager = new CassandraSinkSchemaManager(
                schema,
                Arrays.asList("country_code", "state_province"),
                Collections.EMPTY_LIST);
        Assert.fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testFieldsIsEmpty() {
        final CassandraSchema schema = new CassandraSchema(KEY_SPACE, TABLE, Collections.EMPTY_LIST);
        final CassandraSinkSchemaManager schemaManager = new CassandraSinkSchemaManager(
                schema,
                Arrays.asList("country_code", "state_province"),
                Collections.EMPTY_LIST);
        Assert.fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testPartitionKeysIsEmpty() {
        final CassandraSchema schema = new CassandraSchema(KEY_SPACE, TABLE, fields);
        final CassandraSinkSchemaManager schemaManager = new CassandraSinkSchemaManager(
                schema,
                Collections.EMPTY_LIST,
                Collections.EMPTY_LIST);
        Assert.fail();
    }

    @Test(expected = IllegalStateException.class)
    public void tesClusterKeyIsNotInFieldNames() {
        final CassandraSchema schema = new CassandraSchema(KEY_SPACE, TABLE, fields);
        final CassandraSinkSchemaManager schemaManager = new CassandraSinkSchemaManager(
                schema,
                Collections.singletonList("country_code"),
                Collections.singletonList(new ClusterKey("non_cluster_field_name", ClusterKey.Order.ASC)));
        Assert.fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testPartitionKeyIsNotInFieldNames() {
        final CassandraSchema schema = new CassandraSchema(KEY_SPACE, TABLE, fields);
        final CassandraSinkSchemaManager schemaManager = new CassandraSinkSchemaManager(
                schema,
                Collections.singletonList("non_field_name"),
                Collections.EMPTY_LIST);
        Assert.fail();
    }

    private void validateCreateTable(final Session session) {
        final ResultSet results = session.execute(getColumnsQuery);

        final List<String> columns = results.all()
                .stream()
                .map(r -> r.getString("column_name"))
                .collect(Collectors.toList());

        Assert.assertTrue(columns.containsAll(
                Arrays.asList("country_code", "state_province", "city", "capacity", "gym_name")));
    }

    private void validateAlterTable(final Session session, List<String> newColumns) {
        final ResultSet results = session.execute(getColumnsQuery);

        final List<String> columns = results.all()
                .stream()
                .map(r -> r.getString("column_name"))
                .collect(Collectors.toList());

        Assert.assertTrue(columns.containsAll(newColumns));
    }

    /**
     * Creates a basic table in Cassandra that can be used for more testing (i.e altering)
     */
    private void createBasicTable() {
        final CassandraSchema schema = new CassandraSchema(KEY_SPACE, TABLE, fields);
        final CassandraSinkSchemaManager schemaManager = new CassandraSinkSchemaManager(
                schema,
                Arrays.asList("country_code", "state_province"),
                Arrays.asList(new ClusterKey("city", ClusterKey.Order.DESC),
                        new ClusterKey("gym_name", ClusterKey.Order.ASC)));
        final String createTableStmt = schemaManager.generateCreateTableStmt();

        try (final Session session = getSession()) {
            session.execute(createTableStmt);
        }
    }

    private Session getSession() {
        final Cluster cluster = CassandraTestUtil.initCluster();
        final Session session = cluster.connect();
        return session;
    }
}
