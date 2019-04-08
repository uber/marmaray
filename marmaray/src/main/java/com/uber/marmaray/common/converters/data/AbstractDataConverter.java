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
package com.uber.marmaray.common.converters.data;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.converters.converterresult.ConverterResult;
import com.uber.marmaray.common.data.ErrorData;
import com.uber.marmaray.common.data.ForkData;
import com.uber.marmaray.common.data.IData;
import com.uber.marmaray.common.data.RDDWrapper;
import com.uber.marmaray.common.data.RawData;
import com.uber.marmaray.common.data.RawDataHelper;
import com.uber.marmaray.common.data.ValidData;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.forkoperator.ForkFunction;
import com.uber.marmaray.common.forkoperator.ForkOperator;
import com.uber.marmaray.common.metrics.DataFeedMetricNames;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.IMetricable;
import com.uber.marmaray.utilities.ErrorExtractor;
import com.uber.marmaray.utilities.ErrorTableUtil;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Implementations of the {@link AbstractDataConverter} interface will convert data records from one schema type
 * to another
 * @param <IS> inputSchema
 * @param <OS> outputSchema
 * @param <ID> inputDataType
 * @param <OD> outputDataType
 */
@Slf4j
public abstract class AbstractDataConverter<IS, OS, ID, OD> implements Serializable, IMetricable {
    public static final long serialVersionUID = 1L;
    public static final Integer VALID_RECORD = 0;
    public static final Integer ERROR_RECORD = 1;
    private static final String SUCCESS = "SUCCESS";
    private static final String FAILURE = "FAILURE";
    private static final String CONVERTER_TAG_NAME = "CONVERTER_NAME";

    @Getter @NonNull
    protected Configuration conf;
    /**
     * If defined then {@link #failureRecordHandler} will be invoked with input record in case of any exception from
     * {@link #convert(Object)}.
     */
    @NonNull
    @Setter
    protected Optional<VoidFunction<ID>> failureRecordHandler = Optional.absent();
    /**
     * If defined then {@link #successRecordHandler} will be invoked with output record if record conversion succeeds
     * with no exceptions from {@link #convert(Object)}.
     */
    @NonNull
    @Setter
    protected Optional<VoidFunction<OD>> successRecordHandler = Optional.absent();

    @NonNull
    protected ErrorExtractor errorExtractor;

    @NonNull
    protected Optional<DataFeedMetrics> topicMetrics = Optional.absent();

    public AbstractDataConverter(@NonNull final Configuration conf, @NonNull final ErrorExtractor errorExtractor) {
        this.conf = conf;
        this.errorExtractor = errorExtractor;
    }

    public void setDataFeedMetrics(@NonNull final DataFeedMetrics topicMetrics) {
        this.topicMetrics = Optional.of(topicMetrics);
    }

    public final RDDWrapper<OD> map(@NonNull final JavaRDD<ID> data) {
        final ForkOperator<IData> converter =
            new ForkOperator<>(data.map(r -> RawDataHelper.getRawData(r)),
                new DataConversionFunction(), this.conf);
        converter.execute();
        if (topicMetrics.isPresent()) {
            reportMetrics(converter.getRddSize(), converter.getNumRddPartitions(), topicMetrics.get());
        }
        // Write error records.
        ErrorTableUtil.writeErrorRecordsToErrorTable(data.context(), this.conf, Optional.absent(),
            new RDDWrapper<>(converter.getRDD(ERROR_RECORD).map(r -> (ErrorData) r), converter.getCount(ERROR_RECORD)),
            errorExtractor);

        return new RDDWrapper<>(converter.getRDD(VALID_RECORD).map(r -> ((ValidData<OD>) r).getData()),
                converter.getCount(VALID_RECORD));
    }

    protected abstract List<ConverterResult<ID, OD>> convert(@NonNull ID data) throws Exception;

    private void reportMetrics(final long rddSize,
                               final int numPartitions,
                               @NonNull final DataFeedMetrics topicMetrics) {
        final Map<String, String> tags = ImmutableMap.of(CONVERTER_TAG_NAME, this.getClass().getName());
        topicMetrics.createLongMetric(DataFeedMetricNames.RDD_PARTITION_SIZE,
            rddSize / Math.max(1, numPartitions), tags);
        topicMetrics.createLongMetric(DataFeedMetricNames.NUM_RDD_PARTITIONS, numPartitions, tags);
    }

    public class DataConversionFunction extends ForkFunction<IData> {

        public DataConversionFunction() {
            registerKeys(Arrays.asList(VALID_RECORD, ERROR_RECORD));
        }

        @Override
        protected List<ForkData<IData>> process(final IData record) {
            if (!(record instanceof RawData)) {
                throw new JobRuntimeException("Illegal data type :" + record.getClass());
            }

            final RawData<ID> rawData = (RawData<ID>) record;

            List<ConverterResult<ID, OD>> results;

            try {
                results = convert(rawData.getData());
            } catch (RuntimeException re) {
                throw new JobRuntimeException(re);
            } catch (Exception e) {
                results = Collections.singletonList(new ConverterResult<ID, OD>(rawData.getData(), e.getMessage()));
            }

            final List<ForkData<IData>> forkData = new ArrayList<>();

            results.stream().forEach(t -> {
                    if (t.getErrorData().isPresent()) {
                        processRecordHandler(AbstractDataConverter.this.failureRecordHandler,
                                t.getErrorData().get().getRawData().getData(), FAILURE);

                        forkData.add(new ForkData<>(Arrays.asList(ERROR_RECORD), t.getErrorData().get()));
                    }

                    if (t.getSuccessData().isPresent()) {
                        processRecordHandler(AbstractDataConverter.this.successRecordHandler,
                                t.getSuccessData().get().getData(), SUCCESS);
                        forkData.add(new ForkData<>(Arrays.asList(VALID_RECORD), t.getSuccessData().get()));
                    }
                });
            return forkData;
        }

        private <T> void processRecordHandler(@NonNull final Optional<VoidFunction<T>> recordHandler,
                                              @NonNull final T data, @NotEmpty final String recordHandlerType) {
            if (recordHandler.isPresent()) {
                try {
                    recordHandler.get().call(data);
                } catch (Exception fe) {
                    log.error("exception in :" + recordHandlerType , fe);
                }
            }
        }
    }
}
