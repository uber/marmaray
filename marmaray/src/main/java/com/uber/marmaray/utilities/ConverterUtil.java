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

package com.uber.marmaray.utilities;

import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import lombok.NonNull;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * {@link ConverterUtil} is a utility class to convert different types of payloads
 */
public final class ConverterUtil {

    private ConverterUtil() {
        throw new JobRuntimeException("This utility class should never be instantiated");
    }

    public static byte[] convertData(@NonNull final AvroPayload avroPayload) throws IOException {
        // We use JsonEncoder explicitly to guarantee if ever needed we can convert back to GenericRecord
        // There are cases where calling toString() on GenericRecord doesn't allow conversion back
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            final GenericRecord record = avroPayload.getData();
            final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), out, false);
            final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());

            writer.write(record, encoder);
            encoder.flush();
            out.flush();
            return out.toByteArray();
        }
    }
}
