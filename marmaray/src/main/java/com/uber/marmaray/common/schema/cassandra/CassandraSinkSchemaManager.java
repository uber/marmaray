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

import com.google.common.base.Optional;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import java.util.List;

@Slf4j
public class CassandraSinkSchemaManager extends CassandraSchemaManager {

    public CassandraSinkSchemaManager(@NonNull final CassandraSchema schema,
                                      @NonNull final List<String> partitionKeys,
                                      @NonNull final List<ClusterKey> clusteringKeys,
                                      @NonNull final Optional<Long> ttl,
                                      @NonNull final Optional<DataFeedMetrics> dataFeedMetrics,
                                      @NonNull final Optional<Long> timestamp,
                                      final boolean orderRequired) {
        super(schema, partitionKeys, clusteringKeys, ttl, dataFeedMetrics, timestamp, orderRequired);
    }

    public CassandraSinkSchemaManager(@NonNull final CassandraSchema schema,
                                      @NonNull final List<String> partitionKeys,
                                      @NonNull final List<ClusterKey> clusteringKeys,
                                      @NonNull final Optional<Long> timestamp) {
        super(schema, partitionKeys, clusteringKeys, Optional.absent(), Optional.absent(), timestamp, false);
    }
}
