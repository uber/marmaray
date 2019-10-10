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

import org.apache.hudi.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;

import java.util.Map;
import java.util.Optional;

/**
 * Helper class to change default behavior for {@link WriteStatus}
 */
public class HoodieWriteStatus extends WriteStatus {

    private long totalRecords;

    public HoodieWriteStatus(Boolean trackSuccessRecords, Double failureFraction) {
        super(trackSuccessRecords, failureFraction);
    }

    /**
     * Overriding {@link #markSuccess(HoodieRecord, Option)} to avoid caching
     * {@link org.apache.hudi.common.model.HoodieKey} for successfully written hoodie records.
     */
    @Override
    public void markSuccess(final HoodieRecord record, final Option<Map<String, String>> optionalRecordMetadata) {
        this.totalRecords++;
    }

    @Override
    public long getTotalRecords() {
        return super.getTotalRecords() + this.totalRecords;
    }
}
