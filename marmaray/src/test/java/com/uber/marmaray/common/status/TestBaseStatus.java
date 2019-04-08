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

import java.util.Arrays;
import java.util.Collections;
import java.util.InvalidPropertiesFormatException;

public class TestBaseStatus {

    @Test
    public void testGetStatus() {

        // Nothing is a success
        BaseStatus status = new BaseStatus();
        Assert.assertEquals(IStatus.Status.SUCCESS, status.getStatus());

        // Test sticky failure
        status = new BaseStatus();
        status.setStatus(IStatus.Status.SUCCESS);
        status.setStatus(IStatus.Status.FAILURE);
        status.setStatus(IStatus.Status.SUCCESS);
        Assert.assertEquals(IStatus.Status.FAILURE, status.getStatus());

        // Test sticky in_progress
        status = new BaseStatus();
        status.setStatus(IStatus.Status.SUCCESS);
        status.setStatus(IStatus.Status.IN_PROGRESS);
        status.setStatus(IStatus.Status.SUCCESS);
        status.setStatus(IStatus.Status.SUCCESS);
        Assert.assertEquals(IStatus.Status.IN_PROGRESS, status.getStatus());

        // Test one of each, failure has priority
        status = new BaseStatus();
        status.setStatus(IStatus.Status.SUCCESS);
        status.setStatus(IStatus.Status.FAILURE);
        status.setStatus(IStatus.Status.IN_PROGRESS);
        status.setStatus(IStatus.Status.SUCCESS);
        Assert.assertEquals(IStatus.Status.FAILURE, status.getStatus());
    }

    @Test
    public void testGetThrowables() {
        // no throwables
        BaseStatus status = new BaseStatus();
        Assert.assertEquals(Collections.emptyList(), status.getExceptions());

        // one throwable
        final Exception e1 = new IllegalStateException("This is a test");
        status = new BaseStatus();
        status.addException(e1);
        Assert.assertEquals(Collections.singletonList(e1), status.getExceptions());

        // multiple throwables stay in order
        final Exception e2 = new NullPointerException("This is a second test");
        final Exception e3 = new InvalidPropertiesFormatException("This is a third test");
        status = new BaseStatus();
        status.addException(e1);
        status.addExceptions(Arrays.asList(e2, e3));
        Assert.assertEquals(Arrays.asList(e1, e2, e3), status.getExceptions());

    }
}
