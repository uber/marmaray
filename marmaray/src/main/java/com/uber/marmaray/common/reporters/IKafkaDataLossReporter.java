package com.uber.marmaray.common.reporters;

import org.hibernate.validator.constraints.NotEmpty;

/**
 * {@link IKafkaDataLossReporter} reports Kafka data loss
 */
public interface IKafkaDataLossReporter {
    void reportDataLoss(@NotEmpty final String kafkaTopicName,
                        final long totalNumberOfMessagesLost);
}
