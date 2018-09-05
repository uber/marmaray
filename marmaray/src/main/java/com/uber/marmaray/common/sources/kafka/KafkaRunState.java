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

import com.google.common.base.Optional;
import com.uber.marmaray.common.sources.IRunState;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * It holds the run state of the kafka run. This state needs to be persisted into checkpoint manager.
 */
@AllArgsConstructor
public class KafkaRunState implements IRunState<KafkaRunState> {
    @Getter
    private Map<Integer, Long> partitionOffsets;

    /**
     * It sets partition offset.
     */
    public void setPartitionOffset(final int partition, final long offset) {
        this.partitionOffsets.put(partition, offset);
    }

    /**
     * Get offset for a partition.
     */
    public Optional<Long> getPartitionOffset(final int partition) {
        if (this.partitionOffsets.containsKey(partition)) {
            return Optional.of(this.partitionOffsets.get(partition));
        } else {
            return Optional.absent();
        }
    }
}
