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
package com.uber.marmaray.common.metrics;

import com.uber.marmaray.common.data.RDDWrapper;
import com.uber.marmaray.common.schema.cassandra.CassandraPayload;

import java.util.List;

public class CassandraPayloadRDDSizeEstimator {
    private final int NO_OF_SAMPLE_ROWS = 1000;

    public long estimateTotalSize(final RDDWrapper<CassandraPayload> rdd) {
        final long totalRows = rdd.getCount();

        final List<CassandraPayload> sampleRows = rdd.getData().takeSample(true, NO_OF_SAMPLE_ROWS);

        final long byteSize = sampleRows
                .stream()
                .map(element -> element.estimateRowSize())
                .reduce((size, accumulator) -> size + accumulator)
                .orElse(0);

        final long totalSize = (long) (byteSize * (((totalRows) * 1.0) / (NO_OF_SAMPLE_ROWS)));

        return totalSize;
    }
}
