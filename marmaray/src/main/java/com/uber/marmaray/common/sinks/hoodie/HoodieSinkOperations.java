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

import com.uber.marmaray.common.configuration.HoodieConfiguration;
import lombok.NonNull;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * Helper class which invokes various operations before / after certain {@link HoodieSink} actions. See individual
 * operations for more details.
 */
public class HoodieSinkOperations {

    /**
     * Gets executed before calling {@link HoodieSink}'s underlying commit action. All the parquet write operations are
     * guaranteed to finish before this. Only thing left is the final commit file creation.
     */
    public void preCommitOperations(@NonNull final HoodieConfiguration hoodieConfiguration,
        @NotEmpty final String commitTime) {
        // do nothing.
    }
}
