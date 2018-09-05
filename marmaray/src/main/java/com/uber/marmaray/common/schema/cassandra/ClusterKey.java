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
package com.uber.marmaray.common.schema.cassandra;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.uber.marmaray.utilities.StringTypes;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.Serializable;
import java.util.List;

/**
 * {@link ClusterKey} defines a clustering key in Cassandra as well as the ordering of the key.
 * We assume by default that the order is ascending.
 */
@AllArgsConstructor
@EqualsAndHashCode
public class ClusterKey implements Serializable {
    private static final Splitter splitter = Splitter.on(StringTypes.COLON);

    @Getter @NotEmpty final String name;
    @Getter final Order order;

    public enum Order {
        ASC,
        DESC
    }

    public String toString() {
        return name + StringTypes.SPACE + order;
    }

    /*
     * Parse a string for the name and the order delimited by the delimiter
     */
    public static ClusterKey parse(@NotEmpty final String value) {
        final List<String> tokens = splitter.splitToList(value);
        Preconditions.checkState(tokens.size() <= 2);

        if (tokens.size() == 1) {
            return new ClusterKey(tokens.get(0), Order.ASC);
        } else {
            return new ClusterKey(tokens.get(0), Order.valueOf(tokens.get(1)));
        }
    }
}

