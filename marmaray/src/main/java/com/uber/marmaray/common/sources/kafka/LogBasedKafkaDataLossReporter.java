package com.uber.marmaray.common.sources.kafka;

import com.uber.marmaray.common.reporters.IKafkaDataLossReporter;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotEmpty;

@Slf4j
public class LogBasedKafkaDataLossReporter implements IKafkaDataLossReporter {

    public void reportDataLoss(@NotEmpty final String kafkaTopicName, final long totalNumberOfMessagesLost) {
        log.info("Kafka topic hitting loss: {} . Num messages lost: {}.",
                kafkaTopicName, totalNumberOfMessagesLost);
    }
}
