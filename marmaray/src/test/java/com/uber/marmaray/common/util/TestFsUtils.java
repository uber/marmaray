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

import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.utilities.FSUtils;
import com.google.common.base.Optional;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.stream.IntStream;

public class TestFsUtils {
    private FileSystem fileSystem;

    @Before
    public void initClass() throws IOException {
        final Configuration conf = new Configuration();
        this.fileSystem = FSUtils.getFs(conf, Optional.absent());
    }

    @Test
    public void testDeleteMetadataFiles() throws IOException {
        final String metadataPath = FileTestUtil.getTempFolder();
        Long currentTime = System.currentTimeMillis();

        IntStream.range(0, 7).forEach(iteration -> {
                try {
                    final Long newTime = currentTime + iteration;
                    final String fileLocation = new Path(metadataPath, newTime.toString()).toString();
                    this.fileSystem.create(new Path(fileLocation));
                } catch (final IOException e) {
                    Assert.fail("IOException occurred while creating files");
                }
        });

        final FileStatus[] fileStatuses = this.fileSystem.listStatus(new Path(metadataPath));
        Assert.assertEquals(7, fileStatuses.length);

        FSUtils.deleteHDFSMetadataFiles(fileStatuses, this.fileSystem, 4, false);

        final int numExpectedRemaining = 4;
        final FileStatus[] remaining = this.fileSystem.listStatus(new Path(metadataPath));
        Assert.assertEquals(numExpectedRemaining, remaining.length);

        IntStream.range(0, 7)
                .forEach(iteration -> {
                    try {
                        final Long newTime = currentTime + iteration;

                        if (iteration < numExpectedRemaining - 1) {
                            Assert.assertFalse(this.fileSystem.exists(new Path(metadataPath, newTime.toString())));
                        } else {
                            Assert.assertTrue(this.fileSystem.exists(new Path(metadataPath, newTime.toString())));
                        }
                    } catch (final IOException e) {
                        Assert.fail("IOException occurred while validating files");
                    }
                });

        // fake delete, this just prints to console what would have been deleted if we really executed the deletion
        FSUtils.deleteHDFSMetadataFiles(fileStatuses, this.fileSystem, 2, true);
        Assert.assertEquals(numExpectedRemaining, remaining.length);
    }
}
