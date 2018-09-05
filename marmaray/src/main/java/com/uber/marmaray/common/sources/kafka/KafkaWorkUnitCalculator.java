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
package com.uber.marmaray.common.sources.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.uber.marmaray.common.configuration.KafkaSourceConfiguration;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.metadata.IMetadataManager;
import com.uber.marmaray.common.metadata.StringValue;
import com.uber.marmaray.common.metrics.ChargebackMetricType;
import com.uber.marmaray.common.metrics.DataFeedMetricNames;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.IChargebackCalculator;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.common.sources.IWorkUnitCalculator;
import com.uber.marmaray.common.sources.kafka.KafkaWorkUnitCalculator.KafkaWorkUnitCalculatorResult;
import com.uber.marmaray.utilities.StringTypes;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.uber.marmaray.utilities.KafkaUtil.getEarliestLeaderOffsets;
import static com.uber.marmaray.utilities.KafkaUtil.getKafkaConsumer;
import static com.uber.marmaray.utilities.KafkaUtil.getLatestLeaderOffsets;
import static com.uber.marmaray.utilities.KafkaUtil.getTopicPartitions;

/**
 * {@link KafkaWorkUnitCalculator} uses previous job run state to compute work units for the current run and also
 * creates run state for next run. Previous {@link KafkaRunState} should be set using
 * {@link #initPreviousRunState(IMetadataManager)} before computing work units. {@link #computeWorkUnits()} will
 * compute work units and will create {@link KafkaWorkUnitCalculatorResult} to hold next run state and work units for .
 * Take a look at {@link KafkaSourceConfiguration} for supported configuration settings.
 */
@Slf4j
@RequiredArgsConstructor
public class KafkaWorkUnitCalculator implements IWorkUnitCalculator<OffsetRange, KafkaRunState,
    KafkaWorkUnitCalculatorResult, StringValue> {

    public static final String KAFKA_METADATA_PREFIX = "kafka_metadata";
    public static final String KAFKA_METADATA_WITH_SEPARATOR = KAFKA_METADATA_PREFIX + StringTypes.COLON;
    public static final String PARTITION_TAG = "partition";
    public static final String TOTAL_PARTITION = "total";

    @Getter
    private final KafkaSourceConfiguration conf;
    @Getter
    @Setter
    private KafkaBootstrapOffsetSelector offsetSelector = new KafkaBootstrapOffsetSelector();
    @Getter
    private Optional<KafkaRunState> previousRunState = Optional.absent();

    private Optional<DataFeedMetrics> topicMetrics = Optional.absent();

    private Optional<IChargebackCalculator> chargebackCalculator = Optional.absent();

    @Override
    public void setDataFeedMetrics(@NonNull final DataFeedMetrics topicMetrics) {
        this.topicMetrics = Optional.of(topicMetrics);
    }

    @Override
    public void setJobMetrics(@NonNull final JobMetrics jobMetrics) {
        // ignored
    }

    @Override
    public void setChargebackCalculator(@NonNull final IChargebackCalculator chargebackCalculator) {
        this.chargebackCalculator = Optional.of(chargebackCalculator);
    }

    @Override
    public void initPreviousRunState(@NonNull final IMetadataManager<StringValue> metadataManager) {
        final String topicName = this.conf.getTopicName();
        final Map<Integer, Long> metadata = new HashMap<>();
        final String topicSpecificName = getTopicSpecificMetadataKey(topicName);
        final List<String> toDelete = new LinkedList<>();
        metadataManager.getAllKeys().forEach(key -> {
                if (key.startsWith(topicSpecificName)) {
                    // this is my specific topic
                    metadata.put(Integer.parseInt(key.substring(topicSpecificName.length())),
                        Long.parseLong(metadataManager.get(key).get().getValue()));
                } else if (key.startsWith(KAFKA_METADATA_WITH_SEPARATOR)) {
                    // this is a specific topic, but not mine. ignore.
                    assert true;
                } else if (key.startsWith(KAFKA_METADATA_PREFIX)) {
                    // this is unspecified topic
                    metadata.put(Integer.parseInt(key.substring(KAFKA_METADATA_PREFIX.length())),
                        Long.parseLong(metadataManager.get(key).get().getValue()));
                    // delete the old, unspecified metadata
                    toDelete.add(key);
                }
            });
        toDelete.forEach(metadataManager::remove);
        this.previousRunState = Optional.of(new KafkaRunState(metadata));
    }

    @VisibleForTesting
    String getTopicSpecificMetadataKey(@NotEmpty final String topicName) {
        return String.format("%s%s:", KAFKA_METADATA_WITH_SEPARATOR, topicName);
    }

    @Override
    public void saveNextRunState(@NonNull final IMetadataManager<StringValue> metadataManager,
                                 final KafkaRunState nextRunState) {
        final String topicName = this.conf.getTopicName();
        final String topicSpecificName = getTopicSpecificMetadataKey(topicName);
        nextRunState.getPartitionOffsets().entrySet().forEach(
            entry -> {
                metadataManager.set(topicSpecificName + entry.getKey(), new StringValue(entry.getValue().toString()));
            });
    }

    @Override
    public KafkaWorkUnitCalculatorResult computeWorkUnits() {
        final String topicName = this.conf.getTopicName();
        final KafkaConsumer kafkaConsumer = getKafkaConsumer(this.conf.getKafkaParams());

        final Set<TopicPartition> topicPartitions;
        final Map<TopicPartition, Long> earliestLeaderOffsets;
        final Map<TopicPartition, Long> latestLeaderOffsets;
        try {
            // Retrieve topic partitions and leader offsets.
            topicPartitions = getTopicPartitions(kafkaConsumer, topicName);
            earliestLeaderOffsets = getEarliestLeaderOffsets(kafkaConsumer, topicName, topicPartitions);
            latestLeaderOffsets = getLatestLeaderOffsets(kafkaConsumer, topicName, topicPartitions);
        } finally {
            kafkaConsumer.close();
        }

        // Read checkpointed topic partition offsets and update it with newly added partitions.
        final Map<Integer, Long> oldPartitionOffsets = readExistingPartitionOffsets();
        if (oldPartitionOffsets.isEmpty()) {
            // If it's a first run then initialize new partitions with latest partition offsets.
            return new KafkaWorkUnitCalculatorResult(Collections.emptyList(),
                new KafkaRunState(this.offsetSelector.getPartitionOffsets(this.conf,
                    latestLeaderOffsets.keySet(), earliestLeaderOffsets, latestLeaderOffsets)));
        }
        updatePartitionStartOffsets(oldPartitionOffsets, earliestLeaderOffsets, latestLeaderOffsets);

        // compute new messages per partition.
        long totalNewMessages = 0;
        final List<PartitionMessages> partitionMessages = new ArrayList<>(latestLeaderOffsets.size());
        for (Entry<TopicPartition, Long> entry : latestLeaderOffsets.entrySet()) {
            if (!oldPartitionOffsets.containsKey(entry.getKey().partition())) {
                log.error("Unable to find offsets for topic {} partition {}",
                    entry.getKey().topic(), entry.getKey().partition());
                continue;
            }
            final long messages = entry.getValue() - oldPartitionOffsets.get(entry.getKey().partition());
            log.debug("topicPartition:{}:messages:{}:latestOffset:{}", entry.getKey(), messages, entry.getValue());
            if (messages == 0) {
                continue;
            }
            totalNewMessages += messages;
            partitionMessages.add(new PartitionMessages(entry.getKey(), messages));
        }
        if (partitionMessages.isEmpty()) {
            // No messges to read.
            log.info("No new offsets are found. :{}", topicName);
            return new KafkaWorkUnitCalculatorResult(Collections.emptyList(), new KafkaRunState(oldPartitionOffsets));
        }
        final List<OffsetRange> workUnits =
            calculatePartitionOffsetRangesToRead(partitionMessages, oldPartitionOffsets,
                totalNewMessages);
        // compute run state for the next run.
        final KafkaRunState nextRunState = createNextRunState(workUnits);
        final KafkaWorkUnitCalculatorResult kafkaWorkUnitCalculatorResult =
            new KafkaWorkUnitCalculatorResult(workUnits, nextRunState);

        computeRunMetrics(latestLeaderOffsets, nextRunState, workUnits);
        log.info("workunits: {}", kafkaWorkUnitCalculatorResult);
        return kafkaWorkUnitCalculatorResult;
    }

    private List<OffsetRange> calculatePartitionOffsetRangesToRead(
        @NonNull final List<PartitionMessages> partitionMessages,
        @NonNull final Map<Integer, Long> partitionStartOffsets, final long numMessages) {
        // This will make sure that we can read more messages from partition with more than average messages per
        // partition at the same time we will read all the messages from partition with less than avg messags.
        Collections.sort(partitionMessages);
        final long maxMessagesToRead = this.conf.getMaxMessagesToRead();
        log.info("topicName:{}:newMessages:{}:maxMessagesToRead:{}", this.conf.getTopicName(), numMessages,
            maxMessagesToRead);
        final boolean hasExtraMessages = numMessages > maxMessagesToRead;
        final long numMessagesToRead = Math.min(numMessages, maxMessagesToRead);

        final List<OffsetRange> offsetRanges = new ArrayList<>(partitionMessages.size());
        long pendingMessages = numMessagesToRead;
        int pendingPartitions = partitionMessages.size();
        for (final PartitionMessages m : partitionMessages) {
            final long numMsgsToBeSelected;
            if (!hasExtraMessages) {
                numMsgsToBeSelected = m.getMessages();
            } else {
                numMsgsToBeSelected = Math.min(pendingMessages / pendingPartitions, m.getMessages());
                pendingMessages -= numMsgsToBeSelected;
                pendingPartitions--;
            }
            if (numMsgsToBeSelected > 0) {
                offsetRanges.add(OffsetRange.create(m.getTopicPartition(),
                    partitionStartOffsets.get(m.getTopicPartition().partition()),
                    partitionStartOffsets.get(m.getTopicPartition().partition()) + numMsgsToBeSelected));
            }
        }
        return offsetRanges;
    }

    // Helper method to read existing KafkaRunState.
    private Map<Integer, Long> readExistingPartitionOffsets() {
        if (!this.previousRunState.isPresent()) {
            throw new JobRuntimeException("Previous run state is not set.");
        }
        final Map<Integer, Long> ret = this.previousRunState.get().getPartitionOffsets();
        log.info("existing partition offset :{}", ret);
        return ret;
    }

    private KafkaRunState createNextRunState(@NonNull final List<OffsetRange> workUnits) {
        final Map<Integer, Long> partitionOffsets = new HashMap<>();
        workUnits.forEach(
            offsetRange -> {
                final int partition = offsetRange.partition();
                if (partitionOffsets.containsKey(partition)) {
                    partitionOffsets
                        .put(partition, Math.max(partitionOffsets.get(partition), offsetRange.untilOffset()));
                } else {
                    partitionOffsets.put(partition, offsetRange.untilOffset());
                }
            }
        );
        return new KafkaRunState(partitionOffsets);
    }

    /*
        It will discover new topic partitions and raise an alert if we lost messages for a topic-partition.
     */
    private void updatePartitionStartOffsets(@NonNull final Map<Integer, Long> partitionOffsetMap,
                                             @NonNull final Map<TopicPartition, Long> earliestTPOffsets,
                                             @NonNull final Map<TopicPartition, Long> latestTPOffsets) {
        if (!partitionOffsetMap.isEmpty()) {
            earliestTPOffsets.entrySet().stream().forEach(
                entry -> {
                    final TopicPartition topicPartition = entry.getKey();
                    if (!partitionOffsetMap.containsKey(topicPartition.partition())) {
                        // New partition is found.
                        log.info("Found new partition for topic:{}:partition:{}", topicPartition.topic(),
                            topicPartition.partition());
                        partitionOffsetMap.put(topicPartition.partition(), entry.getValue());
                    } else if (entry.getValue() > partitionOffsetMap.get(topicPartition.partition())) {
                        // TODO(omkar): raise an alert?
                        final String errMsg =
                            String.format(
                                "DATA_LOSS:MISSED_KAFKA_MESSAGES:topic:%s:partition:%d:startOffset:%d:endOffset:%d",
                                topicPartition.topic(), topicPartition.partition(),
                                partitionOffsetMap.get(topicPartition.partition()), entry.getValue());
                        log.error(errMsg);
                        throw new JobRuntimeException(errMsg);
                    }
                }
            );
        }
    }

    /*
        Creates metrics for the current execution based on the source.
     */
    private void computeRunMetrics(@NonNull final Map<TopicPartition, Long> latestLeaderOffsets,
                                   @NonNull final KafkaRunState nextRunState,
                                   @NonNull final List<OffsetRange> offsetRanges) {
        if (!this.topicMetrics.isPresent()) {
            log.error("No topicMetrics, unable to produce metrics");
            return;
        }
        final DataFeedMetrics topicMetrics = this.topicMetrics.get();
        final Map<TopicPartition, Long> offsetMap = new HashMap<>();
        final Map<String, String> totalTags = new HashMap<>();
        totalTags.put(PARTITION_TAG, TOTAL_PARTITION);
        final MessageCounters counter = new MessageCounters();
        offsetRanges.forEach(offsetRange -> {
                final Long oldCount = offsetMap.getOrDefault(offsetRange.topicPartition(), 0L);
                offsetMap.put(offsetRange.topicPartition(), oldCount + offsetRange.count());
            });
        latestLeaderOffsets.forEach(
            (topicPartition, leaderOffset) ->
                computePartitionMetrics(
                    topicPartition, leaderOffset, nextRunState,
                    topicMetrics, offsetMap.getOrDefault(topicPartition, 0L), counter)
        );

        topicMetrics.createLongMetric(DataFeedMetricNames.ROWCOUNT_BEHIND,
            counter.getTotalAvailable() - counter.getTotalCurrent(), totalTags);
        topicMetrics.createLongMetric(DataFeedMetricNames.INPUT_ROWCOUNT, counter.getTotalInput(), totalTags);
        if (this.chargebackCalculator.isPresent()) {
            this.chargebackCalculator.get().addCost(
                this.topicMetrics.get().getBaseTags().get(DataFeedMetrics.DATA_FEED_NAME),
                    ChargebackMetricType.ROW_COUNT, counter.getTotalInput());
        }
    }

    /*
        Creates metrics per partition based on current progress in data processing
     */
    private void computePartitionMetrics(@NonNull final TopicPartition topicPartition,
                                         final long leaderOffset,
                                         @NonNull final KafkaRunState nextRunState,
                                         @NonNull final DataFeedMetrics topicMetrics,
                                         @NonNull final Long inputCount,
                                         @NonNull final MessageCounters counter) {
        final Optional<Long> nextPartitionState = nextRunState.getPartitionOffset(topicPartition.partition());
        final Map<String, String> tags = new HashMap<>();
        tags.put(PARTITION_TAG, Integer.toString(topicPartition.partition()));
        if (nextPartitionState.isPresent()) {
            final long current = nextPartitionState.get();
            counter.add(leaderOffset, current, inputCount);

            topicMetrics.createLongMetric(DataFeedMetricNames.AVAILABLE_ROWCOUNT, leaderOffset, tags);
            topicMetrics.createLongMetric(DataFeedMetricNames.CURRENT_STATUS, current, tags);
            topicMetrics.createLongMetric(DataFeedMetricNames.INPUT_ROWCOUNT, inputCount, tags);
        } else {
            topicMetrics.createLongMetric(DataFeedMetricNames.AVAILABLE_ROWCOUNT, leaderOffset, tags);
            topicMetrics.createLongMetric(DataFeedMetricNames.CURRENT_STATUS, leaderOffset, tags);
            topicMetrics.createLongMetric(DataFeedMetricNames.INPUT_ROWCOUNT, 0, tags);
        }
    }

    /**
     * It holds current set of work units and also {@link KafkaRunState} for the next run.
     */
    public final class KafkaWorkUnitCalculatorResult implements
        IWorkUnitCalculator.IWorkUnitCalculatorResult<OffsetRange, KafkaRunState> {

        @Getter
        private final KafkaRunState nextRunState;
        @Getter
        private final List<OffsetRange> workUnits;

        /*
         * We need constructor to be private so that it is created only from KafkaWorkUnitCalculator.
         */
        private KafkaWorkUnitCalculatorResult(@NonNull final List<OffsetRange> workUnits,
                                              @NonNull final KafkaRunState nextRunState) {
            this.nextRunState = nextRunState;
            this.workUnits = workUnits;
        }

        /**
         * @return true if there are work units available for current run.
         */
        @Override
        public boolean hasWorkUnits() {
            return !this.workUnits.isEmpty();
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("offsetRanges=");
            this.workUnits.forEach(
                workUnit -> sb.append(
                    workUnit.partition()).append(":").append(workUnit.fromOffset()).append("->")
                    .append(workUnit.untilOffset()).append(";"));
            return sb.toString();
        }
    }

    // Helper class used for counting and sorting number of messages per partition.
    @AllArgsConstructor
    @EqualsAndHashCode
    private static class PartitionMessages implements Comparable<PartitionMessages> {

        @Getter
        private final TopicPartition topicPartition;
        @Getter
        private final Long messages;

        @Override
        public int compareTo(final PartitionMessages m) {
            return messages.compareTo(m.getMessages());
        }
    }

    // Helper class to track all of the message stats to accumulate
    private static class MessageCounters {

        @Getter
        private long totalAvailable = 0;

        @Getter
        private long totalCurrent = 0;

        @Getter
        private long totalInput = 0;

        private void add(final long available, final long current, final long input) {
            this.totalAvailable += available;
            this.totalCurrent += current;
            this.totalInput += input;
        }
    }

}
