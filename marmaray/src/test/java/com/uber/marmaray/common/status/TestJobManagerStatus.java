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

package com.uber.marmaray.common.status;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class TestJobManagerStatus {

    @Test
    public void testStatuses() {
        // no status is successful
        JobManagerStatus status = new JobManagerStatus();
        Assert.assertEquals(IStatus.Status.SUCCESS, status.getStatus());
        Assert.assertEquals(Collections.emptyMap(), status.getJobStatuses());

        status.addJobStatus("MyJob", new BaseStatus());
        Assert.assertEquals(IStatus.Status.SUCCESS, status.getStatus());
        Assert.assertEquals(1, status.getJobStatuses().size());

        final BaseStatus failedStatus = new BaseStatus();
        final Exception e1 = new NullPointerException("Foo was here!");
        failedStatus.setStatus(IStatus.Status.FAILURE);
        failedStatus.addException(e1);
        status.addJobStatus("MySecondJob", failedStatus);
        Assert.assertEquals(Collections.singletonList(e1), status.getExceptions());
        Assert.assertEquals(IStatus.Status.FAILURE, status.getStatus());
        Assert.assertEquals(2, status.getJobStatuses().size());

    }

}
