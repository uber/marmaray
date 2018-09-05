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
package com.uber.marmaray.common.util;

import com.uber.marmaray.utilities.DateUtil;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;

public class TestDateUtil {

    @Test
    public void testConvertToUTCDate() {
        // We should be able to handle both forward slashes and dashes as separators
        // since we sanitize the input
        final String dateStr = "2017-05-01";
        LocalDate ld = DateUtil.convertToUTCDate(dateStr);
        Assert.assertEquals(2017, ld.getYear());
        Assert.assertEquals(5, ld.getMonth().getValue());
        Assert.assertEquals(1, ld.getDayOfMonth());

        final String dateStr2 = "1998-06-10";
        LocalDate l2 = DateUtil.convertToUTCDate(dateStr2);
        Assert.assertEquals(1998, l2.getYear());
        Assert.assertEquals(6, l2.getMonth().getValue());
        Assert.assertEquals(10, l2.getDayOfMonth());
    }
}
