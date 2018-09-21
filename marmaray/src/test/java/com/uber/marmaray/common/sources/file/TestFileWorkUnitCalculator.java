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

package com.uber.marmaray.common.sources.file;

import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.FileSourceConfiguration;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import lombok.NonNull;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.hibernate.validator.constraints.NotEmpty;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class TestFileWorkUnitCalculator {


    @Test
    public void computeWorkUnitsNoJson() throws Exception{
        final Configuration conf = new Configuration();
        conf.setProperty(FileSourceConfiguration.TYPE, "json");
        conf.setProperty(FileSourceConfiguration.SCHEMA, "{}");
        final Path testDir = Files.createTempDirectory(null);
        try {
            conf.setProperty(FileSourceConfiguration.DIRECTORY, testDir.toString());
            final FileWorkUnitCalculator workUnitCalculator = new FileWorkUnitCalculator(new FileSourceConfiguration(conf));
            final FileWorkUnitCalculator.FileWorkUnitCalculatorResult result = workUnitCalculator.computeWorkUnits();
            // there are no *.json files in this directory
            Assert.assertFalse(result.hasWorkUnits());
        } finally {
            FileUtils.deleteDirectory(testDir.toFile());
        }
    }

    @Test(expected = JobRuntimeException.class)
    public void computeWorkUnitsNoSuchDirectory() {
        final Configuration conf = new Configuration();
        conf.setProperty(FileSourceConfiguration.TYPE, "json");
        conf.setProperty(FileSourceConfiguration.SCHEMA, "{}");
        conf.setProperty(FileSourceConfiguration.DIRECTORY, "path/not/exist");
        final FileWorkUnitCalculator workUnitCalculator = new FileWorkUnitCalculator(new FileSourceConfiguration(conf));
        final FileWorkUnitCalculator.FileWorkUnitCalculatorResult result = workUnitCalculator.computeWorkUnits();
    }

    @Test
    public void computeWorkUnitsSuccess() throws Exception {
        final Path testDir = Files.createTempDirectory(null);
        final String jsonFile1 = "file1.json";
        final String jsonFile2 = "file2.json";
        final String csvFile = "file3.csv";
        try {
            createFile(testDir, jsonFile1);
            createFile(testDir, jsonFile2);
            createFile(testDir, csvFile);
            final Configuration conf = new Configuration();
            conf.setProperty(FileSourceConfiguration.TYPE, "json");
            conf.setProperty(FileSourceConfiguration.SCHEMA, "{}");
            conf.setProperty(FileSourceConfiguration.DIRECTORY, testDir.toString());
            final FileWorkUnitCalculator workUnitCalculator = new FileWorkUnitCalculator(new FileSourceConfiguration(conf));
            final FileWorkUnitCalculator.FileWorkUnitCalculatorResult result = workUnitCalculator.computeWorkUnits();
            Assert.assertEquals(2, result.getWorkUnits().size());
            final Set<String> expectedResults = new HashSet<>(Arrays.asList(jsonFile1, jsonFile2));
            final Set<String> actualResults = result.getWorkUnits().stream()
                .map(FileStatus::getPath)
                .map(org.apache.hadoop.fs.Path::getName)
                .collect(Collectors.toSet());
            Assert.assertEquals(expectedResults, actualResults);
        } finally {
            FileUtils.deleteDirectory(testDir.toFile());
        }
    }

    private void createFile(@NonNull final Path testDir, @NotEmpty final String s) throws Exception {
        final File f = new File(testDir.toFile(), s);
        new FileOutputStream(f).close();
    }

}
