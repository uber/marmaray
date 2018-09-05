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
package com.uber.marmaray.common.util;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import java.io.IOException;

public final class CassandraTestUtil {

    public static void setupCluster() throws ConfigurationException, TTransportException, IOException {
        System.setProperty("cassandra.storagedir", "/tmp/cassandra" + System.nanoTime());
        EmbeddedCassandraServerHelper.startEmbeddedCassandra("/cassandra.yaml");
        final Cluster cluster = initCluster();
        try (final Session session = cluster.connect()) {
            final CQLDataLoader loader = new CQLDataLoader(session);
            loader.load(new ClassPathCQLDataSet("setupTable.cql"));
        }
    }

    public static void teardownCluster() {
        final Cluster cluster = initCluster();
        try (final Session session = cluster.connect()) {
            final CQLDataLoader loader = new CQLDataLoader(session);
            loader.load(new ClassPathCQLDataSet("teardownTable.cql"));
        }
    }

    public static Cluster initCluster() {
        final Cluster cluster = new Cluster.Builder()
                .addContactPoints(CassandraTestConstants.LOCALHOST)
                .withPort(CassandraTestConstants.CASSANDRA_PORT)
                .build();
        return cluster;
    }

}
