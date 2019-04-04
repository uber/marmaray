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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieInsertException;
import com.uber.hoodie.exception.HoodieUpsertException;
import com.uber.hoodie.table.UserDefinedBulkInsertPartitioner;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.HoodieConfiguration;
import com.uber.marmaray.common.converters.data.HoodieSinkDataConverter;
import com.uber.marmaray.common.data.ErrorData;
import com.uber.marmaray.common.data.RDDWrapper;
import com.uber.marmaray.common.data.RawDataHelper;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.metadata.HoodieBasedMetadataManager;
import com.uber.marmaray.common.metadata.IMetadataManager;
import com.uber.marmaray.common.metrics.DataFeedMetricNames;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.common.sinks.ISink;
import com.uber.marmaray.common.sinks.SinkStatManager;
import com.uber.marmaray.common.sinks.SinkStatManager.SinkStat;
import com.uber.marmaray.common.sinks.hoodie.partitioner.DefaultHoodieDataPartitioner;
import com.uber.marmaray.utilities.ErrorTableUtil;
import com.uber.marmaray.utilities.FSUtils;
import com.uber.marmaray.utilities.HoodieSinkErrorExtractor;
import com.uber.marmaray.utilities.HoodieUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import org.hibernate.validator.constraints.NotEmpty;
import scala.Option;
import scala.Tuple2;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * This class should be used when we need to write data to Hoodie storage. Check {@link HoodieSinkOp} for supported
 * operations. {@link HoodieSinkDataConverter} is used to generate {@link HoodieRecord} for given data type. Error records
 * generated as a part of either {@link HoodieRecord} generation or while writing to Hoodie storage will be written to
 * ErrorTable. If {@link HoodieBasedMetadataManager} is passed in then metadata from it will be saved into newly
 * generated hoodie commit file and it will also reset {@link HoodieBasedMetadataManager#shouldSaveChanges()} flag.
 */
@Slf4j
public class HoodieSink implements ISink, scala.Serializable {

    private static final String TABLE_NAME = "table_name";
    private final HoodieConfiguration hoodieConf;
    // It is used for generating HoodieKey from AvroPayload.
    private final HoodieSinkDataConverter hoodieSinkDataConverter;
    private final transient JavaSparkContext jsc;
    private final HoodieSinkOp op;
    private Optional<DataFeedMetrics> dataFeedMetrics = Optional.absent();
    private final Map<String, String> dataFeedMetricsTags = new HashMap<>();
    /**
     * If set to {@link HoodieBasedMetadataManager} then metadata will be retrieved from it and saved into hoodie
     * commit file.
     */
    private final IMetadataManager metadataMgr;
    /**
     * Used for managing sink stats.
     */
    private final SinkStatManager sinkStatMgr;
    private final boolean shouldSaveChangesInFuture;

    @Getter
    @Setter
    @NonNull
    /**
     * If set then it is used for sorting records during {@link HoodieWriteClientWrapper#bulkInsert(JavaRDD, String)}
     */
    private final UserDefinedBulkInsertPartitioner bulkInsertPartitioner;

    @Setter
    @NonNull
    @Getter
    // If set then it is used for creating new hoodie commit.
    private Optional<String> commitTime = Optional.absent();

    @Setter
    @NonNull
    @Getter
    private HoodieSinkOperations hoodieSinkOperations = new HoodieSinkOperations();

    public HoodieSink(@NonNull final HoodieConfiguration hoodieConf,
                      @NonNull final HoodieSinkDataConverter hoodieSinkDataConverter,
                      @NonNull final JavaSparkContext jsc,
                      @NonNull final HoodieSinkOp op,
                      @NonNull final IMetadataManager metadataMgr,
                      @NonNull final Optional<String> defaultDataPartitioner) {
      this(hoodieConf, hoodieSinkDataConverter, jsc, op, metadataMgr, false, defaultDataPartitioner);
    }

    public HoodieSink(@NonNull final HoodieConfiguration hoodieConf,
                      @NonNull final HoodieSinkDataConverter hoodieSinkDataConverter,
                      @NonNull final JavaSparkContext jsc,
                      @NonNull final HoodieSinkOp op,
                      @NonNull final IMetadataManager metadataMgr,
                      final boolean shouldSaveChangesInFuture,
                      @NonNull final Optional<String> defaultDataPartitioner) {
        this.hoodieConf = hoodieConf;
        this.hoodieSinkDataConverter = hoodieSinkDataConverter;
        this.jsc = jsc;
        this.op = op;
        this.metadataMgr = metadataMgr;
        this.sinkStatMgr = new SinkStatManager(this.hoodieConf.getTableName(), this.metadataMgr);
        this.sinkStatMgr.init();
        this.shouldSaveChangesInFuture = shouldSaveChangesInFuture;
        this.bulkInsertPartitioner = getDataPartitioner(this.hoodieConf, defaultDataPartitioner);
    }

    public void setDataFeedMetrics(@NonNull final DataFeedMetrics dataFeedMetrics) {
        this.dataFeedMetrics = Optional.of(dataFeedMetrics);
    }

    public void addDataFeedMetricsTags(@NonNull final Map<String, String> addtionalTags) {
        this.dataFeedMetricsTags.putAll(addtionalTags);
    }

    @Override
    public void setJobMetrics(@NonNull final JobMetrics jobMetrics) {
        // ignored
    }

    @Override
    public void write(@NonNull final JavaRDD<AvroPayload> data) {
        // Generate HoodieRecord from AvroPayload. It may generate error records in the process and should be
        // written to error tables.
        final RDDWrapper<HoodieRecord<HoodieRecordPayload>> hoodieRecords = this.hoodieSinkDataConverter.map(data);
        write(hoodieRecords);
    }

    public void write(@NonNull final RDDWrapper<HoodieRecord<HoodieRecordPayload>> hoodieRecords) {
        /**
         * 1) create new commit -> getOrCreate()
         * 2) insert records -> bulkInsert() / insert() / upsert()
         * 3) commit() -> commit()
         */
        this.initDataset();
        if (this.hoodieConf.shouldAutoTuneParallelism()) {
            calculateAndUpdateParallelism(hoodieRecords.getCount());
        }

        final HoodieWriteConfig hoodieWriteConfig = this.hoodieConf.getHoodieWriteConfig();
        try (final HoodieWriteClientWrapper hoodieWriteClient = getHoodieWriteClient(hoodieWriteConfig)) {
            final String commitTime =
                this.commitTime.isPresent() ? this.commitTime.get() : hoodieWriteClient.startCommit();

            // Handle writes to hoodie. It can be an insert or upsert.
            final HoodieWriteResult result = handleWrite(hoodieWriteClient, hoodieRecords.getData(), commitTime, op);
            writeRecordsAndErrors(result, true);

            commit(hoodieWriteClient, commitTime, result.getWriteStatuses());
        }
    }

    /**
     * Ensure that hoodie dataset is present.
     */
    protected void initDataset() {
        try {
            HoodieUtil.initHoodieDataset(FSUtils.getFs(this.hoodieConf.getConf(),
                Optional.of(this.hoodieConf.getBasePath())), this.hoodieConf);
        } catch (IOException e) {
            log.error("Error initializing hoodie dataset.", e);
            throw new JobRuntimeException("Could not initialize hoodie dataset", e);
        }
    }

    /**
     * If {@link HoodieConfiguration#HOODIE_AUTO_TUNE_PARALLELISM} is enabled then it will use
     * {@link HoodieConfiguration#HOODIE_TARGET_FILE_SIZE} and {@link SinkStatManager#getAvgRecordSize()} to figure
     * out what should be the optimal insert parallelism.
     * @param numRecords
     */
    public boolean updateInsertParallelism(final long numRecords) {
        if (this.hoodieConf.shouldAutoTuneParallelism()) {
            final int newParallelism = calculateNewBulkInsertParallelism(numRecords);
            if (0 < newParallelism) {
                this.hoodieConf.setTableProperty(HoodieConfiguration.HOODIE_INSERT_PARALLELISM,
                    Integer.toString(newParallelism));
                log.info("new hoodie insert parallelism is set to :{}", newParallelism);
                return true;
            }
        }
        return false;
    }

    /**
     * If {@link HoodieConfiguration#HOODIE_AUTO_TUNE_PARALLELISM} is enabled then it will use
     * {@link HoodieConfiguration#HOODIE_TARGET_FILE_SIZE} and {@link SinkStatManager#getAvgRecordSize()} to figure
     * out what should be the optimal bulk insert parallelism.
     * @param numRecords
     */
    public boolean updateBulkInsertParallelism(final long numRecords) {
        if (this.hoodieConf.shouldAutoTuneParallelism()) {
            final int newParallelism = calculateNewBulkInsertParallelism(numRecords);
            if (0 < newParallelism) {
                this.hoodieConf.setTableProperty(HoodieConfiguration.HOODIE_BULKINSERT_PARALLELISM,
                    Integer.toString(newParallelism));
                log.info("new hoodie bulk insert parallelism is set to :{}", newParallelism);
                return true;
            }
        }
        return false;
    }

    @VisibleForTesting
    protected int calculateNewBulkInsertParallelism(final long numRecords) {
        final long avgRecordSize = this.sinkStatMgr.getAvgRecordSize();
        final long targetFileSize = this.hoodieConf.getTargetFileSize();
        final int newParallelism = (int) Math.ceil((numRecords * avgRecordSize * 1.0) / Math.max(1, targetFileSize));
        final int currentParallelism = this.hoodieConf.getBulkInsertParallelism();
        log.info(
                "StatsManager:targetFileSize:{}:avgRecordSize:{}:numRecords:{}:"
                + "newBulkInsertParallelism:{}:currentBulkInsertParallelism:{}",
                targetFileSize, avgRecordSize, numRecords, newParallelism, currentParallelism);
        return newParallelism;
    }

    @VisibleForTesting
    protected HoodieWriteClientWrapper getHoodieWriteClient(@NonNull final HoodieWriteConfig hoodieWriteConfig) {
        final HoodieWriteClient<HoodieRecordPayload> hoodieWriteClient =
            new HoodieWriteClient<HoodieRecordPayload>(this.jsc, hoodieWriteConfig,
                this.hoodieConf.shouldRollbackInFlight());
        return new HoodieWriteClientWrapper(hoodieWriteClient, this.bulkInsertPartitioner);
    }

    /**
     * If {@link #metadataMgr} is defined then it will retrieve metadata info from metadata manager and reset
     * {@link HoodieBasedMetadataManager#shouldSaveChanges()} flag.
     */
    public void commit(@NonNull final HoodieWriteClientWrapper hoodieWriteClient,
                          @NotEmpty final String commitTime,
                          @NonNull final Optional<JavaRDD<WriteStatus>> writesStatuses) {
        this.commit(hoodieWriteClient, commitTime, writesStatuses, this.shouldSaveChangesInFuture);
    }

    public void commit(@NonNull final HoodieWriteClientWrapper hoodieWriteClient,
                          @NotEmpty final String commitTime,
                          @NonNull final Optional<JavaRDD<WriteStatus>> writesStatuses,
                          final boolean shouldSaveChangesInFuture) {
        updateSinkStat(writesStatuses);
        logWriteMetrics(writesStatuses);

        java.util.Optional<HashMap<String, String>> hoodieExtraMetadata = java.util.Optional.empty();
        if (this.metadataMgr instanceof HoodieBasedMetadataManager) {
            // Retrieve metadata from metadata manager and update metadata manager to avoid it creating extra
            // hoodie commit.
            final HoodieBasedMetadataManager hoodieBasedMetadataManager = (HoodieBasedMetadataManager) this.metadataMgr;
            hoodieExtraMetadata = java.util.Optional.of(hoodieBasedMetadataManager.getMetadataInfo());
            if (!shouldSaveChangesInFuture) {
                hoodieBasedMetadataManager.shouldSaveChanges().set(false);
            }
        }
        hoodieSinkOperations.preCommitOperations(this.hoodieConf, commitTime);
        if (writesStatuses.isPresent() || hoodieExtraMetadata.isPresent()) {
            if (writesStatuses.isPresent()) {
                hoodieWriteClient.commit(commitTime, writesStatuses.get(), hoodieExtraMetadata);
            } else {
                hoodieWriteClient.commit(commitTime, this.jsc.emptyRDD(), hoodieExtraMetadata);
            }
        }
    }

    private void calculateAndUpdateParallelism(final long numRecords) {
        switch (this.op) {
            case BULK_INSERT:
            case DEDUP_BULK_INSERT:
                final int newBulkInsertParallelism = calculateNewBulkInsertParallelism(numRecords);
                updateBulkInsertParallelism(newBulkInsertParallelism);
                break;
            case INSERT:
            case DEDUP_INSERT:
                // no-op
                log.warn("Hoodie insert parallelism is not updated.");
                break;
            case UPSERT:
                // no-op
                log.warn("Hoodie upsert parallelism is not updated.");
                break;
            default:
                throw new JobRuntimeException("Cannot update the parallelism for HoodieOP " + this.op);
        }
    }

    private void updateBulkInsertParallelism(final int newParallelism) {
        if (newParallelism > 0) {
            this.hoodieConf.setTableProperty(HoodieConfiguration.HOODIE_BULKINSERT_PARALLELISM,
                    Integer.toString(newParallelism));
            log.info("new hoodie bulk insert parallelism is set to :{}", newParallelism);
        }
    }

    private void logWriteMetrics(final Optional<JavaRDD<WriteStatus>> writesStatuses) {
        if (writesStatuses.isPresent() && this.dataFeedMetrics.isPresent()) {
            final LongAccumulator totalCount = writesStatuses.get().rdd().sparkContext().longAccumulator();
            final LongAccumulator errorCount = writesStatuses.get().rdd().sparkContext().longAccumulator();
            writesStatuses.get().foreach(writeStatus -> {
                    errorCount.add(writeStatus.getFailedRecords().size());
                    totalCount.add(writeStatus.getTotalRecords());
                });
            this.dataFeedMetrics.get().createLongMetric(DataFeedMetricNames.ERROR_ROWCOUNT, errorCount.value(),
                    this.dataFeedMetricsTags);
            this.dataFeedMetrics.get().createLongMetric(DataFeedMetricNames.OUTPUT_ROWCOUNT,
                    totalCount.value() - errorCount.value(), this.dataFeedMetricsTags);
        }
    }

    /**
     * {@link #updateSinkStat(Optional)} will compute {@link SinkStat} and persist changes into {@link IMetadataManager}.
     * As a part of {@link SinkStat} computation; it will compute avg record size for current run.
     * @param writesStatuses
     */
    private void updateSinkStat(final Optional<JavaRDD<WriteStatus>> writesStatuses) {
        if (writesStatuses.isPresent()) {
            final LongAccumulator avgRecordSizeCounter = writesStatuses.get().rdd().sparkContext().longAccumulator();
            final LongAccumulator fileCount = writesStatuses.get().rdd().sparkContext().longAccumulator();
            final LongAccumulator totalSize = writesStatuses.get().rdd().sparkContext().longAccumulator();
            writesStatuses.get().foreach(
                writeStatus -> {
                    final long writeBytes = writeStatus.getStat().getTotalWriteBytes();
                    final long numInserts = writeStatus.getStat().getNumWrites()
                            - writeStatus.getStat().getNumUpdateWrites();
                    if (writeBytes > 0 && numInserts > 0) {
                        avgRecordSizeCounter.add(writeBytes / numInserts);
                    }
                    fileCount.add(1);
                    totalSize.add(writeBytes);
                }
            );
            final long avgRecordSize = (int) avgRecordSizeCounter.avg();
            if (avgRecordSize > 0) {
                log.info("Updating Sink Stat manager : avgRecordSize : {}", avgRecordSize);
                this.sinkStatMgr.getCurrentStat().put(SinkStat.AVG_RECORD_SIZE, Long.toString(avgRecordSize));
            }
            if (this.dataFeedMetrics.isPresent()) {
                this.dataFeedMetrics.get().createLongMetric(DataFeedMetricNames.TOTAL_FILE_COUNT, fileCount.value(),
                    this.dataFeedMetricsTags);
                this.dataFeedMetrics.get().createLongMetric(DataFeedMetricNames.TOTAL_WRITE_SIZE, totalSize.value(),
                    this.dataFeedMetricsTags);
            }
        }
        this.sinkStatMgr.persist();
    }

    public void writeRecordsAndErrors(@NonNull final HoodieWriteResult result,
                                      final boolean isErrorTableEnabled) {
        try {
            if (result.getException().isPresent()) {
                throw result.getException().get();
            }
            if (result.getWriteStatuses().isPresent()) {
                if (isErrorTableEnabled) {
                    // TODO: Can we make this more readable, please?
                    final JavaRDD<Tuple2<HoodieRecord, String>> hoodieRecordAndErrorTupleRDD
                            = result.getWriteStatuses().get()
                            .flatMap(ws -> ws.getFailedRecords().stream().map(fr ->
                                    new Tuple2<>(fr, ws.getErrors().get(fr.getKey()).getMessage())).iterator());

                    final JavaRDD<ErrorData> errorRDD = hoodieRecordAndErrorTupleRDD
                            .map(r -> new ErrorData(r._2, RawDataHelper.getRawData(r._1)));

                    ErrorTableUtil.writeErrorRecordsToErrorTable(this.jsc.sc(),
                            this.hoodieConf.getConf(), Optional.of(this.hoodieConf.getTableName()),
                            new RDDWrapper<>(errorRDD), new HoodieSinkErrorExtractor());
                }
            }
        } catch (HoodieInsertException | HoodieUpsertException e) {
            log.error("Error writing to hoodie", e);
            throw new JobRuntimeException("hoodie write failed :"
                    + (result.getWriteStatuses().isPresent() ? result.getWriteStatuses().get().count() : -1), e);
        } catch (Exception e) {
            throw new JobRuntimeException("Error writing to hoodie", e);
        }
    }

    public HoodieWriteResult handleWrite(
            @NonNull final HoodieWriteClientWrapper writeClient,
            @NonNull final JavaRDD<HoodieRecord<HoodieRecordPayload>> hoodieRecords,
            @NotEmpty final String commitTime,
            @NonNull final HoodieSinkOp op) {

        Optional<JavaRDD<WriteStatus>> writeStatuses = Optional.absent();
        Optional<Exception> exception = Optional.absent();
        final JavaRDD<HoodieRecord<HoodieRecordPayload>> filteredRecords;
        try {
            switch (op) {
                case INSERT:
                    writeStatuses = Optional.of(writeClient.insert(hoodieRecords, commitTime));
                    break;
                case BULK_INSERT:
                    writeStatuses = Optional.of(writeClient.bulkInsert(hoodieRecords, commitTime));
                    break;
                case DEDUP_INSERT:
                    filteredRecords = dedupRecords(writeClient, hoodieRecords);
                    writeStatuses = Optional.of(writeClient.insert(filteredRecords, commitTime));
                    break;
                case DEDUP_BULK_INSERT:
                    filteredRecords = dedupRecords(writeClient, hoodieRecords);
                    writeStatuses = Optional.of(writeClient.bulkInsert(filteredRecords, commitTime));
                    break;
                case UPSERT:
                    writeStatuses = Optional.of(writeClient.upsert(hoodieRecords, commitTime));
                    break;
                default:
                    exception = Optional.of(new JobRuntimeException("Unsupported hoodie sink operation:" + op));
            }
        } catch (Exception e) {
            exception = Optional.of(e);
        }
        return new HoodieWriteResult(writeStatuses, exception);
    }

    private JavaRDD<HoodieRecord<HoodieRecordPayload>> dedupRecords(@NonNull final HoodieWriteClientWrapper writeClient,
        @NonNull final JavaRDD<HoodieRecord<HoodieRecordPayload>> hoodieRecords) {
        return writeClient.filterExists(hoodieRecords).persist(StorageLevel.DISK_ONLY());
    }

    /**
     * Use {@link HoodieConfiguration#HOODIE_DATA_PARTITIONER} for setting desired data partitioner. It will be used in
     * hoodie's bulk insert flow for repartitioning input records before creating new parquet files. For more details
     * see {@link UserDefinedBulkInsertPartitioner}.
     */
    public static UserDefinedBulkInsertPartitioner getDataPartitioner(@NonNull final HoodieConfiguration hoodieConf,
        @NonNull final Optional<String> defaultDataPartitioner) {
        try {
            return (UserDefinedBulkInsertPartitioner) Class.forName(hoodieConf.getHoodieDataPartitioner(
                defaultDataPartitioner.isPresent() ? defaultDataPartitioner.get()
                    : DefaultHoodieDataPartitioner.class.getName())).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | ClassCastException e) {
            throw new JobRuntimeException("exception in initializing data partitioner", e);
        }
    }

    /**
     * This class is a wrapper around hoodie write client to enable testing.
     */
    @VisibleForTesting
    @AllArgsConstructor
    public static class HoodieWriteClientWrapper implements Serializable, Closeable {

        @VisibleForTesting
        @Getter
        private final HoodieWriteClient<HoodieRecordPayload> hoodieWriteClient;
        private final UserDefinedBulkInsertPartitioner bulkInsertPartitioner;

        public String startCommit() {
            return this.hoodieWriteClient.startCommit();
        }

        public void startCommitWithTime(@NotEmpty final String commitTime) {
            this.hoodieWriteClient.startCommitWithTime(commitTime);
        }

        public boolean commit(@NotEmpty final String commitTime, @NonNull final JavaRDD<WriteStatus> writeStatuses,
            final java.util.Optional<HashMap<String, String>> extraMetadata) {
            return this.hoodieWriteClient.commit(commitTime, writeStatuses, extraMetadata);
        }

        public JavaRDD<WriteStatus> insert(@NonNull final JavaRDD<HoodieRecord<HoodieRecordPayload>> records,
            @NotEmpty final String commitTime) {
            return this.hoodieWriteClient.insert(records, commitTime);
        }

        public JavaRDD<WriteStatus> bulkInsert(@NonNull final JavaRDD<HoodieRecord<HoodieRecordPayload>> records,
            @NotEmpty final String commitTime) {
            return this.hoodieWriteClient.bulkInsert(records, commitTime, Option.apply(this.bulkInsertPartitioner));
        }

        public JavaRDD<WriteStatus> upsert(@NonNull final JavaRDD<HoodieRecord<HoodieRecordPayload>> records,
            @NotEmpty final String commitTime) {
            return this.hoodieWriteClient.upsert(records, commitTime);
        }

        public JavaRDD<WriteStatus> upsertPreppedRecords(
                @NonNull final JavaRDD<HoodieRecord<HoodieRecordPayload>> records,
                @NotEmpty final String commitTime) {
            return this.hoodieWriteClient.upsertPreppedRecords(records, commitTime);
        }

        public JavaRDD<WriteStatus> bulkInsertPreppedRecords(
                @NonNull final JavaRDD<HoodieRecord<HoodieRecordPayload>> records,
                @NotEmpty final String commitTime,
                @NonNull final Option<UserDefinedBulkInsertPartitioner> partitioner) {
            return this.hoodieWriteClient.bulkInsertPreppedRecords(records, commitTime, partitioner);
        }

        public void close() {
            this.hoodieWriteClient.close();
        }

        public JavaRDD<HoodieRecord<HoodieRecordPayload>> filterExists(
            final JavaRDD<HoodieRecord<HoodieRecordPayload>> hoodieRecords) {
            return this.hoodieWriteClient.filterExists(hoodieRecords);
        }
    }

    /**
     * Supported hoodie write operations.
     */
    public enum HoodieSinkOp {
        /** {@link HoodieWriteClient#insert(JavaRDD, String)}*/
        INSERT,
        /** {@link HoodieWriteClient#bulkInsert(JavaRDD, String)}*/
        BULK_INSERT,
        /** {@link HoodieWriteClient#insert(JavaRDD, String)} {@link HoodieWriteClient#filterExists(JavaRDD)}*/
        DEDUP_INSERT,
        /** {@link HoodieWriteClient#bulkInsert(JavaRDD, String)} {@link HoodieWriteClient#filterExists(JavaRDD)}*/
        DEDUP_BULK_INSERT,
        /** {@link com.uber.hoodie.HoodieWriteClient#upsert(org.apache.spark.api.java.JavaRDD, java.lang.String)}*/
        UPSERT,
        /** No operation */
        NO_OP
    }

    @Getter
    @AllArgsConstructor
    public class HoodieWriteResult {
        @NonNull
        private final Optional<JavaRDD<WriteStatus>> writeStatuses;
        @NonNull
        private final Optional<Exception> exception;
    }
}
