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
package com.uber.marmaray.common.sinks.hoodie;

import com.google.common.base.Optional;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.exception.HoodieInsertException;
import com.uber.hoodie.exception.HoodieUpsertException;
import com.uber.marmaray.common.configuration.HoodieConfiguration;
import com.uber.marmaray.common.converters.data.HoodieSinkDataConverter;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.metadata.IMetadataManager;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Map;

/**
 * This class should be used when we need to write error data to Hoodie storage. Error writes need a different
 * implementation than {@link HoodieSink} to avoid a loop while writing Errors. If writing errors to Hoodie fails,
 * then we throw a {@link JobRuntimeException}. For more details {@see HoodieSink}
 */
@Slf4j
public class HoodieErrorSink extends HoodieSink {

    public HoodieErrorSink(@NonNull final HoodieConfiguration hoodieConf,
                           @NonNull final HoodieSinkDataConverter hoodieSinkDataConverter,
                           @NonNull final JavaSparkContext jsc,
                           @NonNull final HoodieSinkOp op,
                           @NonNull final IMetadataManager metadataMgr,
                           final boolean shouldSaveChangesInFuture) {
        super(hoodieConf, hoodieSinkDataConverter, jsc, op, metadataMgr, shouldSaveChangesInFuture, Optional.absent());
    }

    public void writeRecordsAndErrors(@NonNull final HoodieWriteResult result) {
        try {
            if (result.getException().isPresent()) {
                throw result.getException().get();
            }
            if (result.getWriteStatuses().isPresent()) {
                final JavaRDD<Map.Entry<HoodieKey, Throwable>> errorRDD = result.getWriteStatuses().get().flatMap(
                    (FlatMapFunction<WriteStatus, Map.Entry<HoodieKey, Throwable>>)
                        writeStatus -> writeStatus
                                           .getErrors()
                                           .entrySet().iterator());
                long errorCount = errorRDD.count();
                if (errorCount > 0) {
                    final Map.Entry<HoodieKey, Throwable> firstRecord = errorRDD.first();
                    final HoodieKey hoodieKey = firstRecord.getKey();
                    final Throwable t = firstRecord.getValue();
                    final String errorMsg = String.format("There are errors when writing to error table. "
                                                    + "First error record -> HoodieKey : %s, Exception : %s",
                        hoodieKey.toString(), t.getMessage());
                    log.error(errorMsg);
                    throw new JobRuntimeException("Failed to write error hoodie records. HoodieWriteResult Exception",
                                                     result.getException().get());
                }
            }
        } catch (HoodieInsertException | HoodieUpsertException e) {
            log.error("Error writing to hoodie", e);
            String errorMsg = "hoodie write failed for errors :"
                              + (result.getWriteStatuses().isPresent() ? result.getWriteStatuses().get().count() : -1);
            throw new JobRuntimeException(errorMsg, e);
        } catch (Exception e) {
            throw new JobRuntimeException("Error writing to hoodie", e);
        }
    }
}
