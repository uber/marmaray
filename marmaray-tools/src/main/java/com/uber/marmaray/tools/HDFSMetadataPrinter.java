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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.metadata.HDFSMetadataManager;
import com.uber.marmaray.common.metadata.StringValue;
import com.uber.marmaray.utilities.CommandLineUtil;
import com.uber.marmaray.utilities.FSUtils;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@Slf4j
public class HDFSMetadataPrinter {
    private static final String METADATA_FILE_OPTION = "mfile";

    public static void main(final String[] args) throws ParseException, IOException {
        final CommandLineParser parser = new GnuParser();
        final Options options = getCLIOptions();
        final CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (final ParseException e) {
            final String cmdLineSyntax = "java -cp [jar_name] com.uber.marmaray.tools.HDFSMetadataPrinter "
                    + "-m [METADATA_FILE]";
            final String header = "This tools prints out all the metadata contents of a HDFS metadata file.";
            final String footer = "For help, please contact the Hadoop Data Platform team";
            CommandLineUtil.printHelp(options, cmdLineSyntax, header, footer);
            throw e;
        }

        final String metadataFilePath = cmd.getOptionValue(METADATA_FILE_OPTION);
        Preconditions.checkState(!Strings.isNullOrEmpty(metadataFilePath));

        log.info("Printing contents of metadata file: " + metadataFilePath);

        final Configuration conf = new Configuration();
        final FileSystem fs = FSUtils.getFs(conf, Optional.absent());
        try (final InputStream is = new BufferedInputStream(fs.open(new Path(metadataFilePath)))) {
            try (final ObjectInputStream input = new ObjectInputStream(is)) {
                final Map<String, StringValue> metadataMap = HDFSMetadataManager.deserialize(input);
                metadataMap.entrySet()
                    .stream()
                    .forEach(entry ->
                            log.info("Key: " + entry.getKey() + " Value: " + entry.getValue().getValue()));
            }
        }
    }

    private static Options getCLIOptions() {
        final Options options = new Options();
        options.addOption(CommandLineUtil.generateOption("m", METADATA_FILE_OPTION, true, "HDFS metadata file", true));
        return options;
    }
}
