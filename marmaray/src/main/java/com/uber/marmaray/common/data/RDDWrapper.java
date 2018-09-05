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
package com.uber.marmaray.common.data;

import lombok.Getter;
import lombok.NonNull;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;

/**
 * Convenience class to wrap RDD of records with its count to avoid multiple calls for count operation. If we need to
 * use count operation for a given RDD more than once in the form of isEmpty or actual count; then this wrapper will be
 * useful.
 * T dataType of RDD records. {@link #data} for more details.
 */
public class RDDWrapper<T> {

    @Getter
    @NonNull
    final JavaRDD<T> data;

    Optional<Long> count;

    public RDDWrapper(@NonNull final JavaRDD<T> data) {
        this.data = data;
        this.count = Optional.absent();
    }

    public RDDWrapper(@NonNull final JavaRDD<T> data, final long count) {
        this.data = data;
        this.count = Optional.of(count);
    }

    public long getCount() {
        if (!count.isPresent()) {
            this.count = Optional.of(this.data.count());
        }
        return count.get();
    }
}
