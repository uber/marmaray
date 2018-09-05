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
package com.uber.marmaray.common.data;

import com.uber.marmaray.common.util.AbstractSparkTest;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

public class TestRDDWrapper extends AbstractSparkTest {

    @Test
    public void testCount() {
        final List<Integer> list1 = Arrays.asList(1,2,3,4,5);
        final JavaRDD<Integer> list1RDD = this.jsc.get().parallelize(list1);

        final RDDWrapper<Integer> rddWrapper1 = new RDDWrapper<Integer>(list1RDD);
        Assert.assertEquals(5, rddWrapper1.getCount());
        Assert.assertEquals(5, rddWrapper1.getData().count());

        final RDDWrapper<Integer> rddWrapper2 = new RDDWrapper<Integer>(list1RDD, 1);
        Assert.assertEquals(1, rddWrapper2.getCount());
        Assert.assertEquals(5, rddWrapper2.getData().count());
    }
}
