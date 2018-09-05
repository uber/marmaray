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
package com.uber.marmaray.common.forkoperator;

import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.exceptions.ForkOperationException;
import com.uber.marmaray.common.data.ForkData;
import com.uber.marmaray.common.util.AbstractSparkTest;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.junit.Assert;
import org.junit.Test;

class DummyForkFunction extends ForkFunction<String> {

    public static final int INVALID_KEY = 0;
    public static final int VALID_KEY = 1;
    public static final int DELETE_KEY = 2;

    public DummyForkFunction() {
        registerKeys(Arrays.asList(INVALID_KEY, VALID_KEY, DELETE_KEY));
    }

    @Override
    protected List<ForkData<String>> process(final String record) {
        final List<Integer> keys = new LinkedList<>();
        if (record.toLowerCase().contains("error")) {
            // errors.
            keys.add(INVALID_KEY);
        } else {
            if (record.toLowerCase().contains("delete")) {
                // deleted records.
                keys.add(DELETE_KEY);
            }
            // all no error records are valid.
            keys.add(VALID_KEY);
        }
        return Collections.singletonList(new ForkData<>(keys, record));
    }
}

class InvalidDummyForkFunction extends DummyForkFunction {

    public static final int UNREGISTERED_KEY = -1;

    @Override
    protected List<ForkData<String>> process(final String record) {
        final List<ForkData<String>> forkData = super.process(record);
        // Add invalid keys.
        forkData.stream().forEach(fd -> fd.getKeys().add(UNREGISTERED_KEY));
        return forkData;
    }
}

public class TestForkOperator extends AbstractSparkTest {

    public List<String> getTestData(int errorRecords, int deletedRecords, int correctRecords) {
        final List<String> testData = new LinkedList<>();
        while (errorRecords-- > 0) {
            testData.add("error" + errorRecords);
        }
        while (deletedRecords-- > 0) {
            testData.add("delete" + deletedRecords);
        }
        while (correctRecords-- > 0) {
            testData.add("correct" + correctRecords);
        }
        return testData;
    }

    public List<String> getInterleavedTestData(int errorRecords, int deletedRecords, int correctRecords) {
        final List<String> testData = new LinkedList<>();

        while (errorRecords > 0 || deletedRecords > 0 || correctRecords > 0) {
            if (errorRecords-- > 0) {
                testData.add("error" + errorRecords);
            }

            if (deletedRecords-- > 0) {
                testData.add("delete" + deletedRecords);
            }

            if (correctRecords-- > 0) {
                testData.add("correct" + correctRecords);
            }
        }
        return testData;
    }

    @Test
    public void testPersistLevel() {
        final Configuration conf = new Configuration();
        final DummyForkFunction forkFunction = new DummyForkFunction();
        final List<String> testData = getTestData(0, 0, 0);
        ForkOperator<String> forkOperator;
        forkOperator = new ForkOperator<>(jsc.get().parallelize(testData), forkFunction, conf);
        // If nothing is specified then it should use default persist level.
        Assert
            .assertEquals(StorageLevel.fromString(forkOperator.DEFAULT_PERSIST_LEVEL), forkOperator.getPersistLevel());

        conf.setProperty(forkOperator.PERSIST_LEVEL, "MEMORY_ONLY");
        forkOperator = new ForkOperator<>(jsc.get().parallelize(testData), forkFunction, conf);
        Assert.assertEquals(StorageLevel.MEMORY_ONLY(), forkOperator.getPersistLevel());
    }

    @Test
    public void invalidForkKeys() {
        final Configuration conf = new Configuration();
        final DummyForkFunction forkFunction = new InvalidDummyForkFunction();
        final int errorRecords = 5, deletedRecords = 7, correctRecords = 9;
        final List<String> testData = getTestData(errorRecords,deletedRecords,correctRecords);
        final JavaRDD<String> testDataRDD = jsc.get().parallelize(testData);
        final ForkOperator<String> forkOperator = new ForkOperator<>(testDataRDD, forkFunction, conf);
        try {
            forkOperator.execute();
            forkOperator.getRDD(DummyForkFunction.DELETE_KEY).count();
            // exception is expected.
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertEquals(ForkOperationException.class, e.getCause().getClass());
            Assert.assertTrue(
                e.getCause().getMessage().contains(Integer.toString(InvalidDummyForkFunction.UNREGISTERED_KEY)));
        }
    }

    @Test
    public void testForkOperatorWithErrorDeletedAndCorrectRecords() {
        final Configuration conf = new Configuration();
        final DummyForkFunction forkFunction = new DummyForkFunction();
        final int errorRecords = 5, deletedRecords = 7, correctRecords = 9;
        final List<String> testData = getTestData(errorRecords, deletedRecords, correctRecords);
        final JavaRDD<String> testDataRDD = jsc.get().parallelize(testData);
        final ForkOperator<String> forkOperator = new ForkOperator<>(testDataRDD, forkFunction, conf);
        forkOperator.execute();
        Assert.assertEquals(errorRecords, forkOperator.getRDD(DummyForkFunction.INVALID_KEY).count());
        Assert.assertEquals(deletedRecords + correctRecords, forkOperator.getRDD(DummyForkFunction.VALID_KEY).count());
        Assert.assertEquals(deletedRecords, forkOperator.getRDD(DummyForkFunction.DELETE_KEY).count());
        Assert.assertEquals(errorRecords, forkOperator.getCount(DummyForkFunction.INVALID_KEY));
        Assert.assertEquals(deletedRecords + correctRecords, forkOperator.getCount(DummyForkFunction.VALID_KEY));
    }

    @Test
    public void testForkOperatorWithInterleavedRecords() {
        // We explicitly interleave error, deleted, and correct records in the list to ensure
        // our fork operator is able to get the data out correctly if they are not explicitly contiguous
        final Configuration conf = new Configuration();
        final DummyForkFunction forkFunction = new DummyForkFunction();
        final int errorRecords = 5, deletedRecords = 7, correctRecords = 9;
        final List<String> testData = getInterleavedTestData(errorRecords, deletedRecords, correctRecords);
        final JavaRDD<String> testDataRDD = jsc.get().parallelize(testData);
        final ForkOperator<String> forkOperator = new ForkOperator<>(testDataRDD, forkFunction, conf);
        forkOperator.execute();
        Assert.assertEquals(errorRecords, forkOperator.getRDD(DummyForkFunction.INVALID_KEY).count());
        Assert.assertEquals(deletedRecords + correctRecords, forkOperator.getRDD(DummyForkFunction.VALID_KEY).count());
        Assert.assertEquals(deletedRecords, forkOperator.getRDD(DummyForkFunction.DELETE_KEY).count());
        Assert.assertEquals(errorRecords, forkOperator.getCount(DummyForkFunction.INVALID_KEY));
        Assert.assertEquals(deletedRecords + correctRecords, forkOperator.getCount(DummyForkFunction.VALID_KEY));
    }

    @Test
    public void testForkOperatorWithOnlyErrorRecords() {
        final Configuration conf = new Configuration();
        final DummyForkFunction forkFunction = new DummyForkFunction();
        final int errorRecords = 5, deletedRecords = 0, correctRecords = 0;
        final List<String> testData = getTestData(errorRecords, deletedRecords, correctRecords);
        final JavaRDD<String> testDataRDD = jsc.get().parallelize(testData);
        final ForkOperator<String> forkOperator = new ForkOperator<>(testDataRDD, forkFunction, conf);
        forkOperator.execute();
        Assert.assertEquals(errorRecords, forkOperator.getRDD(DummyForkFunction.INVALID_KEY).count());
        Assert.assertEquals(0, forkOperator.getRDD(DummyForkFunction.VALID_KEY).count());
        Assert.assertEquals(0, forkOperator.getRDD(DummyForkFunction.DELETE_KEY).count());
        Assert.assertEquals(errorRecords, forkOperator.getCount(DummyForkFunction.INVALID_KEY));
        Assert.assertEquals(0, forkOperator.getCount(DummyForkFunction.VALID_KEY));
    }

    @Test
    public void testForkOperatorWithOnlyErrorAndCorrectRecords() {
        final Configuration conf = new Configuration();
        final DummyForkFunction forkFunction = new DummyForkFunction();
        final int errorRecords = 5, deletedRecords = 0, correctRecords = 3;
        final List<String> testData = getTestData(errorRecords, deletedRecords, correctRecords);
        final JavaRDD<String> testDataRDD = jsc.get().parallelize(testData);
        final ForkOperator<String> forkOperator = new ForkOperator<>(testDataRDD, forkFunction, conf);
        forkOperator.execute();
        Assert.assertEquals(errorRecords, forkOperator.getRDD(DummyForkFunction.INVALID_KEY).count());
        Assert.assertEquals(correctRecords, forkOperator.getRDD(DummyForkFunction.VALID_KEY).count());
        Assert.assertEquals(0, forkOperator.getRDD(DummyForkFunction.DELETE_KEY).count());
        Assert.assertEquals(errorRecords, forkOperator.getCount(DummyForkFunction.INVALID_KEY));
        Assert.assertEquals(correctRecords, forkOperator.getCount(DummyForkFunction.VALID_KEY));
    }
}
