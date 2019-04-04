package com.uber.marmaray.common.spark;

import com.uber.marmaray.common.util.AbstractSparkTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

public class TestMarmarayKryoSerializer extends AbstractSparkTest {

    @Test
    public void testExceptionSerialization() {
        final List<Object> exceptions = new LinkedList<>();
        final int numberOfExceptions = 10;
        IntStream.range(0, numberOfExceptions)
                .forEach(i -> exceptions.add(new Exception("test-" + i)));
        final List<Object> exceptionList = this.jsc.get().parallelize(exceptions).map(o -> o).collect();
        Assert.assertEquals(numberOfExceptions, exceptionList.size());
    }
}
