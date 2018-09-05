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

package com.uber.marmaray.common.spark;

import com.google.common.base.Optional;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.apache.avro.Schema;

@ToString
@AllArgsConstructor
public class SparkArgs {

    /**
     * Avro schemas to be added to spark context for serialization
     */
    @Getter
    @NonNull
    private final Optional<List<Schema>> avroSchemas;
    /**
     * User serialization classes to be added for kryo serialization
     */
    @Getter
    @NonNull
    private final List<Class> userSerializationClasses;
    /**
     * Other spark properties provided to override defaults
     */
    @Getter
    @NonNull
    private final Map<String, String> overrideSparkProperties;

    /**
     * Hadoop Configuration to be added as a resource to SparkContext
     */
    @Getter
    @NonNull
    private final org.apache.hadoop.conf.Configuration hadoopConfiguration;
}
