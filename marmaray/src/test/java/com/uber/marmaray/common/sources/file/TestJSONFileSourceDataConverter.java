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

package com.uber.marmaray.common.sources.file;

import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.converters.converterresult.ConverterResult;
import com.uber.marmaray.common.util.AbstractSparkTest;
import com.uber.marmaray.utilities.KafkaSourceConverterErrorExtractor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestJSONFileSourceDataConverter extends AbstractSparkTest {

    @Test
    public void convert() throws Exception {
        final Configuration conf = new Configuration();
        final Schema schema = new Schema.Parser().parse(
            getClass().getClassLoader().getResourceAsStream("schemas/schemasource/myTestSchema.1.avsc"));
        final JSONFileSourceDataConverter converter = new JSONFileSourceDataConverter(
            conf, new KafkaSourceConverterErrorExtractor(), schema);
        final List<ConverterResult<String, AvroPayload>> results =
            converter.convert("{\"firstName\": 112, \"lastName\": \"Lname\", \"address\": {\"line1\": \"1234 Main St\", \"city\": \"The City\", \"zip\": 12345}}");
        Assert.assertEquals(1, results.size());
        Assert.assertFalse(results.get(0).getErrorData().isPresent());
        final GenericRecord gr = results.get(0).getSuccessData().get().getData().getData();
        // should be able to convert int to string
        Assert.assertEquals(new Utf8("112"), gr.get("firstName"));
        Assert.assertEquals(new Utf8("Lname"), gr.get("lastName"));
    }

    @Test
    public void convertIncorrectSchema() throws Exception {
        final String badData = "{\"firstName\": \"Eric\", \"lastName\": \"Sayle\", \"address\": {\"line1\": \"1234 Main St\", \"city\": \"The City\", \"zip\": \"MyZip\"}}";
        final Configuration conf = new Configuration();
        final Schema schema = new Schema.Parser().parse(
            getClass().getClassLoader().getResourceAsStream("schemas/schemasource/myTestSchema.1.avsc"));
        final JSONFileSourceDataConverter converter = new JSONFileSourceDataConverter(
            conf, new KafkaSourceConverterErrorExtractor(), schema);
        final List<ConverterResult<String, AvroPayload>> results =
            converter.convert(badData);
        Assert.assertEquals(1, results.size());
        Assert.assertFalse(results.get(0).getSuccessData().isPresent());
        Assert.assertTrue(results.get(0).getErrorData().isPresent());
        Assert.assertEquals(badData, results.get(0).getErrorData().get().getRawData().getData());
    }

}
