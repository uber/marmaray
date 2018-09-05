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
package com.uber.marmaray.common.job;

import com.google.common.base.Optional;
import com.uber.marmaray.common.IPayload;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.JobMetrics;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.NonNull;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

class TestPayload implements IPayload<List<Integer>> {

    @NonNull
    private final List<Integer> numbers;

    public TestPayload(@NonNull final List<Integer> numbers) {
        this.numbers = numbers;
    }

    @Override
    public List<Integer> getData() {
        return this.numbers;
    }
}

class TestSubDag extends JobSubDag {

    public static final String EVEN_TASK = "even";
    public static final String ODD_TASK = "odd";

    private final boolean isEven;
    private final AtomicInteger stepCounter;

    public TestSubDag(final boolean isEven, @NonNull final AtomicInteger stepCounter) {
        super(isEven ? EVEN_TASK : ODD_TASK);
        this.isEven = isEven;
        this.stepCounter = stepCounter;
    }

    @Override
    protected void executeNode(final Optional<IPayload> data) {
        Assert.assertTrue(data.isPresent() && data.get() instanceof TestPayload);
        final int stepNumber = this.stepCounter.incrementAndGet();
        Assert.assertTrue(stepNumber > 1 && stepNumber < 4);
        ((TestPayload) data.get()).getData().stream().forEach(
            number -> {
                Assert.assertTrue(number % 2 == (this.isEven ? 0 : 1));
            }
        );
    }

    @Override
    protected void commitNode() {
        final int stepNumber = this.stepCounter.incrementAndGet();
        Assert.assertTrue(stepNumber == (this.isEven ? 4 : 5));
    }
}

class TestParentSubDag extends JobSubDag {

    private final Map<String, IPayload> childData = new HashMap<>();
    private final AtomicInteger stepCounter;

    public TestParentSubDag(@NonNull final AtomicInteger stepCounter) {
        super("parent");
        this.stepCounter = stepCounter;
        addSubDag(0, new TestSubDag(true, stepCounter));
        addSubDag(1, new TestSubDag(false, stepCounter));
        final String jobName = "testJob";
        setJobMetrics(new JobMetrics(jobName));
        setDataFeedMetrics(new DataFeedMetrics(jobName, new HashMap<>()));
    }

    @Override
    protected void executeNode(final Optional<IPayload> data) {
        Assert.assertTrue(this.stepCounter.incrementAndGet() == 1);
        Assert.assertTrue(data.isPresent());
        final List<Integer> evenList = new LinkedList<>();
        final List<Integer> oddList = new LinkedList<>();
        ((TestPayload) data.get()).getData().stream().forEach(
            number -> {
                if (number % 2 == 0) {
                    evenList.add(number);
                } else {
                    oddList.add(number);
                }
            });
        this.childData.put(TestSubDag.EVEN_TASK, new TestPayload(evenList));
        this.childData.put(TestSubDag.ODD_TASK, new TestPayload(oddList));
    }

    @Override
    protected Optional<IPayload> getDataForChild(final JobSubDag childSubDag) {
        final IPayload childPayload = this.childData.get(childSubDag.getName());
        if (childPayload == null) {
            return Optional.absent();
        } else {
            return Optional.of(childPayload);
        }
    }

    @Override
    protected void commitNode() {
        Assert.assertEquals(6, this.stepCounter.incrementAndGet());
    }
}

public class TestJobSubDag {

    @BeforeClass
    public static void setupClass() {
        final Configuration conf = new Configuration();
        conf.setProperty(ThreadPoolService.NUM_THREADS, "4");
        conf.setProperty(ThreadPoolService.JOB_DAG_THREADS, "2");
        ThreadPoolService.init(conf);
    }

    @AfterClass
    public static void cleanupClass() {
        ThreadPoolService.shutdown(true);
    }

    @Test
    public void testSubDag() {
        final AtomicInteger stepCounter = new AtomicInteger(0);
        final TestParentSubDag parentSubDag = new TestParentSubDag(stepCounter);
        final int count = 100;
        final List<Integer> nums = new ArrayList<Integer>(count);
        for (int i =0; i < count; i++) {
            nums.add(i);
        }
        parentSubDag.execute(Optional.of(new TestPayload(nums)));
        parentSubDag.commit();
        Assert.assertEquals(6, stepCounter.get());
    }
}
