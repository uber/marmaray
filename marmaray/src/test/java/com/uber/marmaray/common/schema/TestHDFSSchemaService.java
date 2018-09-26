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

package com.uber.marmaray.common.schema;

import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.HDFSSchemaServiceConfiguration;
import com.uber.marmaray.common.exceptions.InvalidDataException;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.util.AbstractSparkTest;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Assert;
import org.junit.Test;

public class TestHDFSSchemaService extends AbstractSparkTest {

    public static final String SCHEMA_NAME = "myTestSchema";

    @Test
    public void testSchema() throws Exception {
        final HDFSSchemaService ss = getHdfsSchemaService();
        final Schema schema1 = ss.getSchema(SCHEMA_NAME, 1);
        final GenericRecord data1 =
            new GenericRecordBuilder(schema1).set("firstName", "Eric").set("lastName", "Sayle").build();
        final byte[] bytes1 = ss.getWriter(SCHEMA_NAME, 1).write(data1);
        final GenericRecord output1 = ss.getReader(SCHEMA_NAME, 1).read(bytes1);
        Assert.assertEquals(output1.get("firstName").toString(), "Eric");
        Assert.assertEquals(output1.get("lastName").toString(), "Sayle");

        final Schema schema2 = ss.getSchema(SCHEMA_NAME);
        final GenericRecord data2 =
                new GenericRecordBuilder(schema2).set("firstName", "Eason").set("lastName", "Lu").set("middleName", "Fitzgerald").build();
        final byte[] bytes2 = ss.getWriter(SCHEMA_NAME).write(data2);
        final GenericRecord output2 = ss.getReader(SCHEMA_NAME).read(bytes2);
        Assert.assertEquals(output2.get("firstName").toString(), "Eason");
        Assert.assertEquals(output2.get("lastName").toString(), "Lu");
        Assert.assertEquals(output2.get("middleName").toString(), "Fitzgerald");
    }

    @Test(expected = InvalidDataException.class)
    public void testInvalidGR() throws Exception {
        final HDFSSchemaService ss = getHdfsSchemaService();
        final Schema wrongSchema = ss.getSchema("wrongSchema");
        final GenericRecord data =
            new GenericRecordBuilder(wrongSchema).set("foo", "boo").build();
        ss.getWriter(SCHEMA_NAME, 1).write(data);
    }

    @Test
    public void testInvalidBytes() throws Exception {
        final HDFSSchemaService ss = getHdfsSchemaService();
        final Schema wrongSchema = ss.getSchema("wrongSchema");
        final GenericRecord data =
            new GenericRecordBuilder(wrongSchema).set("foo", "boo").build();
        final byte[] bytes = ss.getWriter("wrongSchema", 1).write(data);
        try {
            ss.getReader(SCHEMA_NAME, 1).read(bytes);
            Assert.fail("Didn't throw error trying to read data");
        } catch (InvalidDataException e) {
            // pass
        }
    }

    @Test(expected = JobRuntimeException.class)
    public void testSchemaNotFound() throws Exception {
        final HDFSSchemaService ss = getHdfsSchemaService();
        ss.getSchema("schemaDNE");
    }

    private HDFSSchemaService getHdfsSchemaService() {
        final Configuration conf = new Configuration();
        conf.setProperty(HDFSSchemaServiceConfiguration.PATH, "src/test/resources/schemas/schemasource");
        return new HDFSSchemaService(conf);
    }
}
