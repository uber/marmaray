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
package com.uber.marmaray.common.metadata;

import com.google.common.base.Optional;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.IMetricable;
import com.uber.marmaray.common.metrics.JobMetrics;
import lombok.NonNull;

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

/**
 * {@link IMetadataManager} describes the generic contract that any implementing metadata manager must adhere to
 * and provides basic get/set operations.
 *
 * The {@link IMetadataManager#saveChanges()} method may be a no-op depending on the implementation of the metadata
 * manager.  Some implementations will keep all changes in memory until a job completes and changes will be persisted
 * while other metadata managers will write through the changes when {@link IMetadataManager#set(String, AbstractValue)}
 * is invoked.  Please see the comments and details for each implementation for further details.
 */
public interface IMetadataManager<T extends AbstractValue> extends Serializable, IMetricable {
    void set(@NonNull final String key, @NonNull final T value);
    Optional<T> remove(@NonNull final String key);
    Optional<T> get(@NonNull final String key);
    void saveChanges() throws IOException;
    Set<String> getAllKeys();

    @Override
    void setDataFeedMetrics(@NonNull final DataFeedMetrics dataFeedMetrics);

    @Override
    void setJobMetrics(@NonNull final JobMetrics jobMetrics);
}
