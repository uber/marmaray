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
package com.uber.marmaray.common;

import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.marmaray.common.exceptions.JobRuntimeException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import lombok.NonNull;

import java.io.IOException;
import java.util.Optional;

/**
 * {@link HoodieErrorPayload} is a class to represent error payloads written by Hoodie.
 */
public class HoodieErrorPayload implements HoodieRecordPayload<HoodieErrorPayload> {
    private final GenericRecord record;

    public HoodieErrorPayload(@NonNull final GenericRecord record) {
        this.record = record;
    }

    @Override
    public Optional<IndexedRecord> getInsertValue(final Schema schema) throws IOException {
        final Optional<GenericRecord> record = getRecord();
        return record.map(r -> HoodieAvroUtils.rewriteRecord(r, schema));
    }

    protected Optional<GenericRecord> getRecord() {
        return Optional.of(this.record);
    }

    @Override
    public HoodieErrorPayload preCombine(final HoodieErrorPayload hoodieErrorPayload) {
        throw new JobRuntimeException("Not implemented yet!!");
    }

    @Override
    public Optional<IndexedRecord> combineAndGetUpdateValue(final IndexedRecord indexedRecord, final Schema schema)
        throws IOException {
        throw new JobRuntimeException("Not implemented yet!!");
    }
}
