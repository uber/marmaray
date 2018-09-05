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
package com.uber.marmaray.common.converters;

import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.converters.converterresult.ConverterResult;
import com.uber.marmaray.common.converters.data.AbstractDataConverter;
import com.uber.marmaray.common.data.RDDWrapper;
import com.uber.marmaray.common.exceptions.InvalidDataException;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.util.AbstractSparkTest;
import com.uber.marmaray.utilities.ErrorExtractor;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.spark.SparkException;
import org.hibernate.validator.constraints.NotEmpty;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

@Slf4j
public class TestAbstractDataConverter extends AbstractSparkTest {

    public static String SUCCESS = "success";
    public static String INVALID_DATA = "invalid_data";
    public static String RUNTIME_EXCEPTION = "runtime_exception";

    @Test
    public void testExceptionHandling() {
        final int successRecords = 5;
        final int invalidDataRecords = 7;
        final int runtimeExceptionRecords = 1;

        final List<String> inputList = new ArrayList<>();
        // Adding only success & invalid_data records.
        IntStream.range(0, successRecords).forEach(i -> inputList.add(SUCCESS));
        IntStream.range(0, invalidDataRecords).forEach(i -> inputList.add(INVALID_DATA));

        final MockAbstractDataConverter mockConverter = new MockAbstractDataConverter(new Configuration(), new ErrorExtractor());
        final RDDWrapper<String> result = mockConverter.map(this.jsc.get().parallelize(inputList));
        Assert.assertEquals(successRecords, result.getCount());

        // Adding runtime exception records. This should fail the spark job.
        IntStream.range(0, runtimeExceptionRecords).forEach(i -> inputList.add(RUNTIME_EXCEPTION));
        try {
            mockConverter.map(this.jsc.get().parallelize(inputList));
            Assert.fail("expecting error here");
        } catch (Exception e) {
            Assert.assertEquals(SparkException.class, e.getClass());
            Assert.assertEquals(JobRuntimeException.class, e.getCause().getClass());
            Assert.assertEquals(JobRuntimeException.class, e.getCause().getCause().getClass());
            Assert.assertEquals(RUNTIME_EXCEPTION, e.getCause().getCause().getMessage());
        }

    }

    private static class MockAbstractDataConverter extends AbstractDataConverter<Schema, Schema, String, String> {

        MockAbstractDataConverter(@NonNull final Configuration conf, ErrorExtractor errorExtractor) {
            super(conf, errorExtractor);
        }

        @Override
        protected List<ConverterResult<String, String>> convert(@NotEmpty final String data) throws Exception {
            if (SUCCESS.equals(data)) {
                return Arrays.asList(new ConverterResult(data));
            } else if (INVALID_DATA.equals(data)) {
                throw new InvalidDataException(data);
            } else {
                throw new JobRuntimeException(data);
            }
        }
    }
}
