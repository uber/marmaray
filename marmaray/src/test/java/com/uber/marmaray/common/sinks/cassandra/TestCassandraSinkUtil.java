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
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.CassandraSinkConfiguration;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.converters.data.CassandraSinkDataConverter;
import com.uber.marmaray.common.converters.schema.CassandraSchemaConverter;
import com.uber.marmaray.common.schema.cassandra.CassandraSchema;
import com.uber.marmaray.common.schema.cassandra.CassandraSinkSchemaManager;
import com.uber.marmaray.common.schema.cassandra.ClusterKey;
import com.uber.marmaray.common.util.AbstractSparkTest;
import com.uber.marmaray.common.util.AvroPayloadUtil;
import com.uber.marmaray.common.util.CassandraTestConstants;
import com.uber.marmaray.common.util.CassandraTestUtil;
import com.uber.marmaray.utilities.SchemaUtil;
import com.uber.marmaray.utilities.StringTypes;
import com.uber.marmaray.utilities.TimestampInfo;
import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static com.uber.marmaray.common.util.CassandraTestConstants.KEY_SPACE;
import static com.uber.marmaray.common.util.CassandraTestConstants.TABLE;

public class TestCassandraSinkUtil extends AbstractSparkTest {

    protected static final String TEST_TIMESTAMP = "10000";

    protected CassandraSinkConfiguration initializeConfiguration(boolean excludeField,
                                                               boolean hasTimestampField) {
        final Configuration conf = new Configuration();
        conf.setProperty(CassandraSinkConfiguration.DATACENTER, "test_dc");
        conf.setProperty(CassandraSinkConfiguration.DISABLE_QUERY_UNS, "true");
        conf.setProperty(CassandraSinkConfiguration.INITIAL_HOSTS, "localhost");
        conf.setProperty(CassandraSinkConfiguration.TABLE_NAME, CassandraTestConstants.TABLE);
        conf.setProperty(CassandraSinkConfiguration.KEYSPACE, CassandraTestConstants.KEY_SPACE);
        conf.setProperty(CassandraSinkConfiguration.CLUSTER_NAME, "testCluster");
        conf.setProperty(CassandraSinkConfiguration.PARTITION_KEYS, "int_field");
        conf.setProperty(CassandraSinkConfiguration.CLUSTERING_KEYS, "string_field");

        // we always exclude the boolean field for now, can modify this in future to exclude a specific field
        if (!excludeField) {
            conf.setProperty(CassandraSinkConfiguration.COLUMN_LIST,
                    Joiner.on(",").join(AvroPayloadUtil.getSchemaFields()));
        } else {
            conf.setProperty(CassandraSinkConfiguration.COLUMN_LIST,
                    Joiner.on(",").join(AvroPayloadUtil.getSchemaFields(CassandraTestConstants.BOOLEAN_FIELD)));
        }

        conf.setProperty(CassandraSinkConfiguration.NATIVE_TRANSPORT_PORT, "9142");
        return new CassandraSinkConfiguration(conf);
    }

    protected void validateCassandraTable(final int expectedNumRows,
                                        boolean excludeBoolField,
                                        final boolean checkTimestampField) {
        final Cluster cluster = CassandraTestUtil.initCluster();
        try (final Session session = cluster.connect()) {
            final String query = "SELECT * FROM " + CassandraTestConstants.KEY_SPACE + ". " + CassandraTestConstants.TABLE;
            final List<Row> rows = session.execute(query).all();
            Assert.assertEquals(expectedNumRows, rows.size());

            for (int i = 0; i < expectedNumRows; i++) {
                final Row row = rows.get(i);
                final Integer intObj = (Integer) row.getObject(CassandraTestConstants.INT_FIELD);
                final String stringObj = (String) row.getObject(CassandraTestConstants.STRING_FIELD);
                Assert.assertEquals(Integer.valueOf(stringObj), intObj);

                if (!excludeBoolField) {
                    final Boolean boolObj = (Boolean) row.getObject(CassandraTestConstants.BOOLEAN_FIELD);
                    Assert.assertTrue(boolObj);
                }

                if (checkTimestampField) {
                    final Object timestampObj = row.getObject(SchemaUtil.DISPERSAL_TIMESTAMP);

                    if (timestampObj instanceof Long) {
                        Assert.assertEquals(Long.parseLong(TEST_TIMESTAMP), timestampObj);
                    } else {
                        Assert.assertEquals(TEST_TIMESTAMP, timestampObj.toString());
                    }
                }
            }
        }
    }
}
