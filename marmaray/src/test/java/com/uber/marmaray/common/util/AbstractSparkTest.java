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

import com.google.common.base.Optional;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.utilities.FSUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.hibernate.validator.constraints.NotEmpty;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

@Slf4j
public class AbstractSparkTest {

    protected Optional<JavaSparkContext> jsc = Optional.absent();
    protected Optional<SparkSession> spark = Optional.absent();
    protected Optional<SQLContext> sqlContext = Optional.absent();
    protected Optional<FileSystem> fileSystem = Optional.absent();
    protected static final String JOB_NAME = "test-job";

    @Before
    public void setupTest() {
        this.spark = Optional.of(SparkTestUtil.getSparkSession(getSparkConf(AbstractSparkTest.class.getName())));
        this.jsc = Optional.of(new JavaSparkContext(this.spark.get().sparkContext()));
        this.sqlContext = Optional.of(SQLContext.getOrCreate(this.jsc.get().sc()));
        try {
            this.fileSystem = Optional.of(FSUtils.getFs(new Configuration()));
        } catch (IOException e) {
            log.error("Cannot initialize FileSystem object", e);
        }
    }

    protected SparkConf getSparkConf(@NotEmpty final String appName) {
        return SparkTestUtil.getSparkConf(appName);
    }

    @After
    public void teardownTest() {
        if (this.spark.isPresent()) {
            this.spark.get().stop();
            this.spark = Optional.absent();
        }

        if (this.jsc.isPresent()) {
            this.jsc.get().stop();
            this.jsc = Optional.absent();
        }

        if (this.sqlContext.isPresent()) {
            this.sqlContext = Optional.absent();
        }

        if (this.fileSystem.isPresent()) {
            try {
                this.fileSystem.get().close();
            } catch (IOException e) {
                log.error("Cannot close FileSystem object", e);
            }
            this.fileSystem = Optional.absent();
        }
    }

    protected String getAppId() {
        return jsc.get().getConf().getAppId();
    }
}
