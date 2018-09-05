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

import com.uber.marmaray.common.IPayload;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CassandraPayload implements IPayload<List<CassandraDataField>>, Serializable {

    /**
     * No need to model keyspace and table name here, we already have it in the schema metadata
     */
    private final List<CassandraDataField> fields = new ArrayList<>();

    @Override
    public List<CassandraDataField> getData() {
        return this.fields;
    }

    public void addField(final CassandraDataField field) {
        this.fields.add(field);
    }

    public List<ByteBuffer> convertData() {
        return fields.stream().map(field -> field.getValue()).collect(Collectors.toList());
    }

    /**
     * estimate the size of the underlying data payload
     */
    public int estimateRowSize() {
        return convertData()
                .stream()
                .filter((row) -> row != null)
                .map((row) -> row.capacity())
                .reduce((size, accumulator) -> size + accumulator)
                .orElse(0);
    }

}
