package com.uber.marmaray.common.sources.kafka;

import com.uber.marmaray.common.reporters.IKafkaDataLossReporter;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * {@link KafkaOffsetResetter} holds the logic and state to reset Kafka offsets when there is data loss.
 */
public class KafkaOffsetResetter {
    @NonNull
    @Getter
    private final IKafkaOffsetSelector offsetSelector;

    @NonNull
    @Getter
    @Setter
    private IKafkaDataLossReporter kafkaDataLossReporter = new LogBasedKafkaDataLossReporter();

    public KafkaOffsetResetter(final IKafkaOffsetSelector offsetSelector) {
        this.offsetSelector = offsetSelector;
    }

    public KafkaOffsetResetter(final IKafkaOffsetSelector offsetSelector,
                               final IKafkaDataLossReporter kafkaDataLossReporter) {
        this.offsetSelector = offsetSelector;
        this.kafkaDataLossReporter = kafkaDataLossReporter;
    }
}
