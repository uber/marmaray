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

package com.uber.marmaray.common.configuration;

import com.uber.marmaray.utilities.ConfigUtil;
import lombok.NonNull;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class FileSourceConfiguration implements Serializable {

    public static final String FILE_SOURCE_PREFIX = Configuration.MARMARAY_PREFIX + "source.file.";
    public static final String DIRECTORY = FILE_SOURCE_PREFIX + "directory";
    public static final String TYPE = FILE_SOURCE_PREFIX + "type";
    public static final String SCHEMA = FILE_SOURCE_PREFIX + "schema";

    private final Configuration conf;

    public FileSourceConfiguration(@NonNull final Configuration conf) {
        ConfigUtil.checkMandatoryProperties(conf, getMandatoryProperties());
        this.conf = conf;
    }

    public Path getDirectory() {
        return new Path(this.conf.getProperty(DIRECTORY).get());
    }

    public String getType() {
        return this.conf.getProperty(TYPE).get().toLowerCase();
    }

    public String getSchema() {
        return this.conf.getProperty(SCHEMA).get();
    }

    public List<String> getMandatoryProperties() {
        return Arrays.asList(DIRECTORY, TYPE, SCHEMA);
    }

    public FileSystem getFileSystem() throws IOException {
        return FileSystem.get(new HadoopConfiguration(this.conf).getHadoopConf());
    }

}
