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
import com.google.common.base.Preconditions;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.KafkaConfiguration;
import com.uber.marmaray.common.configuration.KafkaSourceConfiguration;
import com.uber.marmaray.common.converters.data.KafkaSourceDataConverter;
import com.uber.marmaray.common.metrics.DataFeedMetricNames;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.common.sources.ISource;
import com.uber.marmaray.common.sources.kafka.KafkaWorkUnitCalculator.KafkaWorkUnitCalculatorResult;
import com.uber.marmaray.utilities.KafkaUtil;
import com.uber.marmaray.utilities.LongAccumulator;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.kafka010.KafkaRDD;
import org.apache.spark.streaming.kafka010.KafkaUtils$;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.hibernate.validator.constraints.NotEmpty;
import scala.Serializable;
import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * It gets work units from {@link KafkaWorkUnitCalculatorResult} as a list of {@link OffsetRange}, reads messages from
 * kafka and returns {@link JavaRDD<AvroPayload>}.
 */
@Slf4j
@AllArgsConstructor
public class KafkaSource implements ISource<KafkaWorkUnitCalculatorResult, KafkaRunState>, Serializable {

    public static final int MIN_PARTITIONS_TO_ENABLE_PARALLEL_BROKER_READ = 256;
    @Getter
    private final KafkaSourceConfiguration conf;
    @Getter
    private transient Optional<JavaSparkContext> jsc = Optional.absent();
    @Getter
    private final KafkaSourceDataConverter dataConverter;
    @Getter
    private final Optional<Function<AvroPayload, Boolean>> startDateFilterFunction;
    @Setter
    @NonNull
    private final Optional<VoidFunction<AvroPayload>> filterRecordHandler;
    @NonNull
    private Optional<DataFeedMetrics> topicMetrics = Optional.absent();

    public KafkaSource(@NonNull final KafkaSourceConfiguration conf,
                       @NonNull final Optional<JavaSparkContext> jsc,
                       @NonNull final KafkaSourceDataConverter dataConverter,
                       @NonNull final Optional<Function<AvroPayload, Boolean>> startDateFilterFunction,
                       @NonNull final Optional<VoidFunction<AvroPayload>> filterRecordHandler) {
        this.conf = conf;
        this.jsc = jsc;
        this.dataConverter = dataConverter;
        this.startDateFilterFunction = startDateFilterFunction;
        this.filterRecordHandler = filterRecordHandler;
    }

    public void setDataFeedMetrics(@NonNull final DataFeedMetrics topicMetrics) {
        this.topicMetrics = Optional.of(topicMetrics);
    }

    public void setJobMetrics(@NonNull final JobMetrics jobMetrics) {
        // ignored
    }

    @Override
    public JavaRDD<AvroPayload> getData(@NonNull final KafkaWorkUnitCalculatorResult workUnitCalc) {
        Preconditions.checkState(workUnitCalc.hasWorkUnits(), "no work to do :" + this.conf.getTopicName());
        final List<OffsetRange> workUnits = workUnitCalc.getWorkUnits();
        /**
         * Since we are not opening more than one connection per "topic+partition"; so single spark partition may end up
         * reading data more than spark's 2G partition limit. In order to avoid this and also to take advantage of all
         * executors we will be repartitioning kafkaData.
         */
        final int readParallelism;
        if (workUnits.size() < this.conf.getReadParallelism()) {
            readParallelism = this.conf.getReadParallelism();
        } else {
            readParallelism = workUnits.size();
        }
        final LongAccumulator totalDataReadInBytes = new LongAccumulator("totalDataRead");
        getJsc().get().sc().register(totalDataReadInBytes);
        final int numPartitions = workUnits.stream().map(r -> r.partition()).collect(Collectors.toSet()).size();
        final Map<Integer, TreeMap<Long, Integer>> kafkaPartitionOffsetToSparkPartitionMap
            = getKafkaPartitionOffsetToOutputSparkPartitionMap(this.conf.getTopicName(), workUnits, readParallelism);
        log.info("using partition offset mapping topic={} : mapping={}", this.conf.getTopicName(),
            kafkaPartitionOffsetToSparkPartitionMap);

        final JavaRDD<byte[]> kafkaDataRead = isParallelBrokerReadEnabled(numPartitions)
            ? readWithMultiReaderPerPartition(workUnits, kafkaPartitionOffsetToSparkPartitionMap)
            : readWithOneReaderPerPartition(workUnits, readParallelism, kafkaPartitionOffsetToSparkPartitionMap);
        final JavaRDD<byte[]> kafkaData = kafkaDataRead.map(val -> {
                totalDataReadInBytes.add((long) val.length);
                return val;
            }
        );
        if (topicMetrics.isPresent()) {
            this.dataConverter.setDataFeedMetrics(this.topicMetrics.get());
        }
        final JavaRDD<AvroPayload> inputRDD = this.dataConverter.map(kafkaData).getData();
        reportReadMetrics(this.topicMetrics, totalDataReadInBytes, numPartitions);
        if (this.startDateFilterFunction.isPresent()) {
            return inputRDD.filter(
                record -> {
                    if (!this.startDateFilterFunction.get().call(record)) {
                        if (this.filterRecordHandler.isPresent()) {
                            this.filterRecordHandler.get().call(record);
                        }
                        return false;
                    }
                    return true;
                });
        }
        return inputRDD;
    }

    public boolean isParallelBrokerReadEnabled(final int numPartitions) {
        final boolean isParallelBrokerReadEnabled =
            this.conf.isParallelBrokerReadEnabled() || numPartitions >= MIN_PARTITIONS_TO_ENABLE_PARALLEL_BROKER_READ;
        log.info("topicName:{} usingParallelBrokerReader:{} partitions:{}", this.conf.getTopicName(),
            isParallelBrokerReadEnabled, numPartitions);
        return isParallelBrokerReadEnabled;
    }

    /**
     * It is used for reading kafka data from kafka brokers by splitting single partition read request into multiple
     * small requests such that single kafka consumer has very less data to read and it can avoid potential shuffle
     * stage used by {@link #readWithOneReaderPerPartition(List, int, Map)} internally.
     */
    public JavaRDD<byte[]> readWithMultiReaderPerPartition(
        @NonNull final List<OffsetRange> workUnits,
        @NonNull final Map<Integer, TreeMap<Long, Integer>> kafkaPartitionOffsetToSparkPartitionMap) {
        final List<OffsetRange> newWorkUnits = new LinkedList<>();
        for (final OffsetRange workUnit : workUnits) {
            if (kafkaPartitionOffsetToSparkPartitionMap.containsKey(workUnit.partition())) {
                /*
                    Here we are trying to recreate OffsetRanges for a given topic-partition identified by workUnit.
                    Let us assume that original workUnit to read from kafka was like this.
                    workUnit -> [partition=1,startOffset=12,endOffset=100]
                    Let us say it gets split into 4 (due to read_parallelism) (with 22 messages each) then this is how
                    it will look like in kafkaPartitionOffsetToSparkPartitionMap
                    {1,{12->0,34->1,56->2,78->3}}.

                    "kafkaPartitionOffsetToSparkPartitionMap" holds Offset to outputSparkPartition mapping for every
                    kafka topic's partition.
                    Top level map's key is kafka topic's partition number.
                        For inner map;
                            key -> kafka partition offset to start new output spark partition
                            value -> output spark partition's number.

                    What this means is for partition 1 create 4 ranges as follows.
                    [partition=1,startOffset=12,endOffset=34]
                    [partition=1,startOffset=34,endOffset=56]
                    [partition=1,startOffset=56,endOffset=78]
                    [partition=1,startOffset=78,endOffset=100]
                 */
                long previousOffset = -1;
                for (Map.Entry<Long, Integer> newRange
                    : kafkaPartitionOffsetToSparkPartitionMap.get(workUnit.partition()).entrySet()) {
                    if (previousOffset > -1) {
                        newWorkUnits.add(
                            OffsetRange.create(workUnit.topicPartition(), previousOffset, newRange.getKey()));
                    }
                    previousOffset = newRange.getKey();
                }
                Preconditions.checkState(previousOffset > -1, "unexpected offset range value");
                if (previousOffset < workUnit.untilOffset()) {
                    newWorkUnits
                        .add(OffsetRange.create(workUnit.topicPartition(), previousOffset, workUnit.untilOffset()));
                }
            }
        }
        return readKafkaData(newWorkUnits).map(e -> e.value());
    }

    public JavaRDD<byte[]> readWithOneReaderPerPartition(
        @NonNull final List<OffsetRange> workUnits,
        final int readParallelism,
        @NonNull final Map<Integer, TreeMap<Long, Integer>> kafkaPartitionOffsetToSparkPartitionMap) {
        return readKafkaData(workUnits)
            .mapToPair(
                new PairFunction<ConsumerRecord<byte[], byte[]>, Integer, byte[]>() {
                    int lastSparkPartition = -1;

                    @Override
                    public Tuple2<Integer, byte[]> call(final ConsumerRecord<byte[], byte[]> v) throws Exception {
                        final int sparkPartition =
                            kafkaPartitionOffsetToSparkPartitionMap.get(v.partition())
                                .floorEntry(v.offset()).getValue();
                        if (lastSparkPartition != sparkPartition) {
                            lastSparkPartition = sparkPartition;
                            log.info("starting new spark partition == kafkaPartition:{} offset:{} sparkPartition:{}",
                                v.partition(), v.offset(), sparkPartition);
                        }
                        return new Tuple2<>(sparkPartition, v.value());
                    }
                }
            ).partitionBy(
                new Partitioner() {
                    @Override
                    public int numPartitions() {
                        return readParallelism;
                    }

                    @Override
                    public int getPartition(final Object key) {
                        return (Integer) key;
                    }
                })
            .values();
    }

    @VisibleForTesting
    protected JavaRDD<ConsumerRecord<byte[], byte[]>> readKafkaData(@NonNull final List<OffsetRange> workUnits) {
        log.info("kafka read workunits #: {} workunits: {}", workUnits.size(), workUnits);
        final HashMap<String, Object> kafkaParams = new HashMap<>(KafkaUtil.getKafkaParams(this.conf));
        KafkaUtils$.MODULE$.fixKafkaParams(kafkaParams);
        // We shuffle work units so that read requests for single topic+partition are evenly spread across tasks in
        // this read stage so that we won't hit the same topic+partition from all tasks.
        Collections.shuffle(workUnits);
        final RDD<ConsumerRecord<byte[], byte[]>> kafkaRDD = new KafkaRDD<byte[], byte[]>(
            this.jsc.get().sc(),
            kafkaParams,
            workUnits.toArray(new OffsetRange[0]),
            Collections.emptyMap(),
            true) {
            // It is overridden to ensure that we don't pin topic+partition consumer to only one executor. This allows
            // us to do parallel reads from kafka brokers.
            @Override
            public Seq<String> getPreferredLocations(final Partition thePart) {
                return new ArrayBuffer<>();
            }

            // We are updating client.id on executor per task to ensure we assign unique ids for it.
            @Override
            public scala.collection.Iterator<ConsumerRecord<byte[], byte[]>> compute(final Partition thePart,
                final TaskContext context) {
                super.kafkaParams().put(KafkaConfiguration.CLIENT_ID, String
                    .format(KafkaConfiguration.DEFAULT_CLIENT_ID,
                        KafkaConfiguration.getClientId()));
                return super.compute(thePart, context);
            }
        };
        return new JavaRDD<>(kafkaRDD, ClassTag$.MODULE$.apply(ConsumerRecord.class));
    }

    /**
     * report read size metrics per partition and total along with individual record stats like average size
     */
    private static void reportReadMetrics(@NonNull final Optional<DataFeedMetrics> topicMetrics,
                                          @NonNull final LongAccumulator totalDataReadInBytes,
                                          final long totalPartitions) {
        if (topicMetrics.isPresent()) {
            topicMetrics.get().createLongMetric(
                DataFeedMetricNames.TOTAL_READ_SIZE, totalDataReadInBytes.getSum(), new HashMap<>());
            topicMetrics.get().createLongMetric(
                DataFeedMetricNames.TOTAL_READ_SIZE_PER_PARTITION,
                totalDataReadInBytes.getSum() / totalPartitions, new HashMap<>());
            topicMetrics.get().createLongMetric(DataFeedMetricNames.AVERAGE_INPUT_RECORD_SIZE,
                (long) totalDataReadInBytes.getAvg(), new HashMap<>());
            topicMetrics.get().createLongMetric(DataFeedMetricNames.MAX_INPUT_RECORD_SIZE,
                totalDataReadInBytes.getMax(), new HashMap<>());
            topicMetrics.get().createLongMetric(DataFeedMetricNames.NUM_INPUT_PARTITIONS,
                totalPartitions, new HashMap<>());
        }
    }

    /**
     * It maps kafka partition's offset ranges to output spark partitions such that each every output spark partition
     * gets equal number of messages.
     */
    private static Map<Integer, TreeMap<Long, Integer>> getKafkaPartitionOffsetToOutputSparkPartitionMap(
        @NotEmpty final String topicName, @NonNull final List<OffsetRange> offsetRanges, final int readParallelism) {

        final List<Integer> outputSparkPartitions = new ArrayList<>(readParallelism);
        IntStream.range(0, readParallelism).forEach(i -> outputSparkPartitions.add(i));
        // We shuffle outputSparkPartitionIds so that reducers won't hit same shuffle service while running
        // "read_parllelism" tasks.
        Collections.shuffle(outputSparkPartitions);
        final Queue<Integer> availableSparkPartitions = new LinkedList<>(outputSparkPartitions);
        return getKafkaPartitionOffsetToOutputSparkPartitionMap(topicName, offsetRanges, availableSparkPartitions,
            readParallelism);
    }

    @VisibleForTesting
    public static Map<Integer, TreeMap<Long, Integer>> getKafkaPartitionOffsetToOutputSparkPartitionMap(
        @NotEmpty final String topicName, @NonNull final List<OffsetRange> offsetRanges,
        @NonNull final Queue<Integer> availableSparkPartitions, final int readParallelism) {
        long totalMessages = 0;
        for (final OffsetRange offsetRange: offsetRanges) {
            totalMessages += (offsetRange.untilOffset() - offsetRange.fromOffset());
        }
        final long messagesPerPartition = (long) Math.max(Math.ceil(totalMessages * 1.0 / readParallelism), 1);
        log.info("total Messages for {} :{}", topicName, totalMessages);

        long remainingSparkPartitionMessageCapacity = messagesPerPartition;
        final Map<Integer, TreeMap<Long, Integer>> retMap = new HashMap<>();
        final Iterator<OffsetRange> offsetRangesI = offsetRanges.iterator();
        OffsetRange currentOffsetRange = offsetRangesI.next();
        while (true) {
            long currentOffsetRangeMsgCnt = currentOffsetRange.untilOffset() - currentOffsetRange.fromOffset();
            if (currentOffsetRangeMsgCnt == 0) {
                if (offsetRangesI.hasNext()) {
                    currentOffsetRange = offsetRangesI.next();
                    continue;
                } else {
                    break;
                }
            }
            if (!retMap.containsKey(currentOffsetRange.partition())) {
                retMap.put(currentOffsetRange.partition(), new TreeMap<>());
            }
            if (remainingSparkPartitionMessageCapacity == 0) {
                remainingSparkPartitionMessageCapacity = messagesPerPartition;
                availableSparkPartitions.poll();
            }
            Preconditions.checkState(!availableSparkPartitions.isEmpty(), "missing output spark partitions");
            retMap.get(currentOffsetRange.partition()).put(currentOffsetRange.fromOffset(),
                availableSparkPartitions.peek());

            if (currentOffsetRangeMsgCnt < remainingSparkPartitionMessageCapacity) {
                remainingSparkPartitionMessageCapacity -= currentOffsetRangeMsgCnt;
                currentOffsetRange = OffsetRange.create(currentOffsetRange.topic(),
                    currentOffsetRange.partition(), currentOffsetRange.fromOffset() + currentOffsetRangeMsgCnt,
                    currentOffsetRange.untilOffset());
            } else {
                currentOffsetRange = OffsetRange.create(currentOffsetRange.topic(),
                    currentOffsetRange.partition(),
                    currentOffsetRange.fromOffset() + remainingSparkPartitionMessageCapacity,
                    currentOffsetRange.untilOffset());
                remainingSparkPartitionMessageCapacity = 0;
            }
        }
        return retMap;
    }
}
