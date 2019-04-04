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
package com.uber.marmaray.common.sinks.hoodie.partitioner;

import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.table.UserDefinedBulkInsertPartitioner;
import lombok.NonNull;
import org.apache.spark.api.java.JavaRDD;
import scala.Serializable;

/**
 * {@link DefaultHoodieDataPartitioner} is used for sorting the records to ensure that all records belonging to single
 * partition are grouped together. for more information also see {@link UserDefinedBulkInsertPartitioner}.
 */
public class DefaultHoodieDataPartitioner implements UserDefinedBulkInsertPartitioner<HoodieRecordPayload>,
    Serializable {

    @Override
    public JavaRDD<HoodieRecord<HoodieRecordPayload>> repartitionRecords(
        @NonNull final JavaRDD<HoodieRecord<HoodieRecordPayload>> javaRDD, final int outputPartitions) {
        return javaRDD.sortBy(
            v1 -> String.format("%s %s", v1.getPartitionPath(), v1.getRecordKey()), true, outputPartitions);
    }
}
