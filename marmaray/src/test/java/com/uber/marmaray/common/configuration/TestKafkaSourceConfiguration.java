/*
 * Copyright (c) 2019 Uber Technologies, Inc.
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

package com.uber.marmaray.common.configuration;

import com.uber.marmaray.common.util.KafkaTestHelper;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TestKafkaSourceConfiguration {

    @Test
    public void constructorWithStartDate() {
        final SimpleDateFormat dateFormat = new SimpleDateFormat(KafkaSourceConfiguration.KAFKA_START_DATE_FORMAT);
        final String startDate = dateFormat.format(new Date());
        final KafkaSourceConfiguration kafkaConfig = KafkaTestHelper.getKafkaSourceConfiguration(
                "topic-name", "broker-address", startDate);
        final long expectedStartDateEpoch = DateTime.parse(startDate,  DateTimeFormat.forPattern(
                KafkaSourceConfiguration.KAFKA_START_DATE_FORMAT).withZoneUTC()).toDate().getTime();
        Assert.assertEquals(kafkaConfig.getStartTime(), expectedStartDateEpoch);
    }

    @Test
    public void constructorWithStartTime() {
        // If both start_date and start_time are specified, start_time should take precedence.
        final SimpleDateFormat dateFormat = new SimpleDateFormat(KafkaSourceConfiguration.KAFKA_START_DATE_FORMAT);
        final String startDate = dateFormat.format(new Date());
        final String startDateAsEpoch = String.valueOf(DateTime.parse(startDate,  DateTimeFormat.forPattern(
                KafkaSourceConfiguration.KAFKA_START_DATE_FORMAT).withZoneUTC()).toDate().getTime());
        final String startTime = String.valueOf(Long.valueOf(startDateAsEpoch) - 500);
        final KafkaSourceConfiguration kafkaConfig = KafkaTestHelper.getKafkaSourceConfiguration(
                "topic-name", "broker-address", startDate, startTime);
        Assert.assertEquals(kafkaConfig.getStartTime(), (long) Long.valueOf(startTime));
    }

    @Test(expected = RuntimeException.class)
    public void constructorWithInvalidStartDate() {
        KafkaTestHelper.getKafkaSourceConfiguration(
                "topic-name", "broker-address", "1970-01-01");
    }

    @Test(expected = RuntimeException.class)
    public void constructorWithInvalidStartTime() {
        final SimpleDateFormat dateFormat = new SimpleDateFormat(KafkaSourceConfiguration.KAFKA_START_DATE_FORMAT);
        final String startDate = dateFormat.format(new Date());
        // The start_date is valid, but the start_time is not.
        KafkaTestHelper.getKafkaSourceConfiguration(
                "topic-name", "broker-address", startDate, "1000000");
    }
}
