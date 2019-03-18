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
import com.uber.marmaray.common.configuration.KafkaSourceConfiguration;
import com.uber.marmaray.common.converters.data.KafkaSourceDataConverter;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.common.sources.ISource;
import com.uber.marmaray.common.sources.kafka.KafkaWorkUnitCalculator.KafkaWorkUnitCalculatorResult;
import com.uber.marmaray.utilities.KafkaUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.hibernate.validator.constraints.NotEmpty;
import scala.Serializable;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * It gets work units from {@link KafkaWorkUnitCalculatorResult} as a list of {@link OffsetRange}, reads messages from
 * kafka and returns {@link JavaRDD<AvroPayload>}.
 */
@Slf4j
@AllArgsConstructor
public class KafkaSource implements ISource<KafkaWorkUnitCalculatorResult, KafkaRunState>, Serializable {

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

    public void setDataFeedMetrics(@NonNull final DataFeedMetrics topicMetrics) {
        // ignored
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
        Map<Integer, TreeMap<Long, Integer>> kafkaPartitionOffsetToSparkPartitionMap
            = getKafkaPartitionOffsetToOutputSparkPartitionMap(this.conf.getTopicName(), workUnits, readParallelism);
        log.info("using partition offset mapping topic={} : mapping={}", this.conf.getTopicName(),
            kafkaPartitionOffsetToSparkPartitionMap);

        final JavaRDD<byte[]> kafkaData = KafkaUtils.<byte[], byte[]>createRDD(
            this.jsc.get(),
            KafkaUtil.getKafkaParams(this.conf),
            workUnits.toArray(new OffsetRange[0]),
            LocationStrategies.PreferConsistent())
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

        final JavaRDD<AvroPayload> inputRDD = this.dataConverter.map(kafkaData).getData();

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

    /**
     * It maps kafka partition's offset ranges to output spark partitions such that each every output spark partition
     * gets equal number of messages.
     */
    @VisibleForTesting
    public static Map<Integer, TreeMap<Long, Integer>> getKafkaPartitionOffsetToOutputSparkPartitionMap(
        @NotEmpty final String topicName, @NonNull final List<OffsetRange> offsetRanges, final int readParallelism) {
        long totalMessages = 0;
        for (final OffsetRange offsetRange: offsetRanges) {
            totalMessages += (offsetRange.untilOffset() - offsetRange.fromOffset());
        }
        final long messagesPerPartition = (long) Math.max(Math.ceil(totalMessages * 1.0 / readParallelism), 1);
        log.info("total Messages for {} :{}", topicName, totalMessages);
        final Map<Integer, TreeMap<Long, Integer>> retMap = new HashMap<>();

        Iterator<OffsetRange> offsetRangesI = offsetRanges.iterator();
        OffsetRange currentOffsetRange = offsetRangesI.next();
        int outputSparkPartition = 0;
        long remainingSparkPartitionMessageCapacity = messagesPerPartition;

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
                outputSparkPartition++;
            }
            retMap.get(currentOffsetRange.partition()).put(currentOffsetRange.fromOffset(), outputSparkPartition);

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
        Preconditions.checkState(outputSparkPartition < readParallelism,
            String.format("number of spark partitions can't be larger than read parallelism : found :%s expected<:%s",
                outputSparkPartition, readParallelism));
        return retMap;
    }
}
