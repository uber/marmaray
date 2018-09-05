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
package com.uber.marmaray.tools;

import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.metadata.HDFSMetadataManager;
import com.uber.marmaray.utilities.CommandLineUtil;
import com.uber.marmaray.utilities.FSUtils;
import java.io.IOException;
import java.util.Comparator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parquet.Preconditions;

@Slf4j
public class HDFSMetadataPruner {
    private static final String HDFS_PATH_LONG_OPTION = "path";
    private static final String NUM_METADATA_FILES_RETAINED_LONG_OPTION = "numFiles";
    private static final String FAKE_DELETE_LONG_OPTION = "fake";
    private static final String HDFS_PATH_SHORT_OPTION = "p";
    private static final String NUM_METADATA_FILES_RETAINED_SHORT_OPTION = "n";
    private static final String FAKE_DELETE_SHORT_OPTION = "f";

    // Metadata file names in HDFS = nanoseconds since epoch so we can sort by name
    private static final Comparator<FileStatus> byTimestampedNameAsc =
            Comparator.comparingLong(f1 -> Long.parseLong(f1.getPath().getName()));

    // Todo - consider putting main functionality in a static utility method
    public static void main(final String[] args) throws ParseException, IOException {
        final CommandLineParser parser = new GnuParser();
        final Options options = getCLIOptions();
        final CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (final ParseException e) {
            final String cmdLineSyntax =
                    String.format("java -cp [jar_name] com.uber.marmaray.tools.HDFSMetadataCuller "
                    + "-%s [METADATA_PATH] -%s [NUM_METADATA_FILES_RETAINED] -%s [FAKE_DELETE_BOOLEAN]",
                    HDFS_PATH_SHORT_OPTION, NUM_METADATA_FILES_RETAINED_SHORT_OPTION, FAKE_DELETE_SHORT_OPTION);
            final String header = "This tool prunes metadata files for an HDFS path by modification time";
            final String footer = "For help, please contact the Hadoop Data Platform team";
            CommandLineUtil.printHelp(options, cmdLineSyntax, header, footer);
            throw e;
        }

        final Path metadataPath = new Path(cmd.getOptionValue(HDFS_PATH_LONG_OPTION));

        final int numFilesToRetain = cmd.hasOption(NUM_METADATA_FILES_RETAINED_LONG_OPTION)
                ? Integer.parseInt(cmd.getOptionValue(NUM_METADATA_FILES_RETAINED_LONG_OPTION))
                : HDFSMetadataManager.DEFAULT_NUM_METADATA_FILES_TO_RETAIN;

        Preconditions.checkState(numFilesToRetain > 0, "Number of files to retain cannot be <= 0");

        final boolean fakeDelete = cmd.hasOption(FAKE_DELETE_LONG_OPTION)
                ? Boolean.parseBoolean(cmd.getOptionValue(FAKE_DELETE_LONG_OPTION))
                : false;

        final Configuration conf = new Configuration();
        final FileSystem fs = FSUtils.getFs(conf);

        if (fs.isDirectory(metadataPath)) {
            final FileStatus[] fileStatuses = fs.listStatus(metadataPath);

            if (fileStatuses.length < numFilesToRetain) {
                log.info("No files were deleted. Number of files ({}) is less than number to retain ({})",
                        fileStatuses.length, numFilesToRetain);
                return;
            }

            FSUtils.deleteHDFSMetadataFiles(fileStatuses, fs, numFilesToRetain, fakeDelete);
        } else {
            log.warn("Cannot prune any files, the path {} is not a directory", metadataPath);
        }
    }

    private static Options getCLIOptions() {
        final Options options = new Options();
        options.addOption(CommandLineUtil.generateOption(HDFS_PATH_SHORT_OPTION,
                HDFS_PATH_LONG_OPTION,
                true,
                "HDFS path",
                true));

        options.addOption(CommandLineUtil.generateOption(NUM_METADATA_FILES_RETAINED_SHORT_OPTION,
                NUM_METADATA_FILES_RETAINED_LONG_OPTION,
                true,
                "number of metadata files to retain",
                false));

        options.addOption(CommandLineUtil.generateOption(FAKE_DELETE_SHORT_OPTION,
                FAKE_DELETE_LONG_OPTION,
                true,
                "fake delete",
                false));
        return options;
    }
}

