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
package com.uber.marmaray.utilities;

import com.google.common.base.Optional;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.HoodieTableConfig;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.marmaray.common.configuration.HoodieConfiguration;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import lombok.NonNull;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.IOException;

/**
 * {@link HoodieUtil} defines utility methods for interacting with Hoodie
 */
public final class HoodieUtil {

    private HoodieUtil() {
        throw new JobRuntimeException("This utility class should never be instantiated");
    }

    /**
     * It initializes hoodie dataset
     * @param fs {@link FileSystem}
     * @param hoodieConf {@link HoodieConfiguration}
     * @throws IOException
     */
    public static void initHoodieDataset(@NonNull final FileSystem fs,
                                          @NonNull final HoodieConfiguration hoodieConf) throws IOException {
        final Path hoodieMetaFolder = new Path(hoodieConf.getBasePath(), HoodieTableMetaClient.METAFOLDER_NAME);
        final Path hoodiePropertiesFile = new Path(hoodieMetaFolder.toString(),
                HoodieTableConfig.HOODIE_PROPERTIES_FILE);
        if (!fs.exists(hoodiePropertiesFile)) {
            HoodieTableMetaClient
                    .initializePathAsHoodieDataset(FSUtils.getFs(hoodieConf.getConf(),
                            Optional.of(hoodieConf.getBasePath())),
                        hoodieConf.getBasePath(), hoodieConf.getHoodieInitProperties());
        }
    }

    public static String getRowKeyFromCellRecord(@NonNull final HoodieRecord cellRecord) {
        final String cellRecordKey = cellRecord.getRecordKey();
        return cellRecordKey.substring(0, cellRecordKey.indexOf(StringTypes.HASHTAG));
    }

    public static <T extends HoodieRecordPayload> JavaRDD<HoodieRecord<T>> combineRecords(
            final JavaRDD<HoodieRecord<T>> records, final Function<HoodieRecord<T>, Object> recordKeyFunc,
            final int parallelism) {
        return records
                .mapToPair(record -> new Tuple2<>(recordKeyFunc.call(record), record))
                .reduceByKey((rec1, rec2) -> {
                        @SuppressWarnings("unchecked")
                        T reducedData = (T) rec1.getData().preCombine(rec2.getData());
                        return new HoodieRecord<T>(rec1.getKey(), reducedData);
                    }, parallelism)
                .map(recordTuple -> recordTuple._2());
    }
}
