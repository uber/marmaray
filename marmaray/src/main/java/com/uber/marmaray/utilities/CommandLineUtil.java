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
package com.uber.marmaray.utilities;

import com.uber.marmaray.common.exceptions.JobRuntimeException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import lombok.NonNull;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.io.IoBuilder;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * {@link CommandLineUtil} provides utility methods to interact with the command line
 */
public class CommandLineUtil {
    public static String executeCommand(@NotEmpty final String cmd) {
        final StringBuffer outputBuffer = new StringBuffer();

        try {
            final Process process = Runtime.getRuntime().exec(cmd);
            process.waitFor();

            String line;
            try (final BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                while ((line = br.readLine()) != null) {
                    outputBuffer.append(line + "\n");
                }
            }
        } catch (IOException | InterruptedException e) {
            throw new JobRuntimeException("Exception occurred while executing command: " + cmd
                    + " Error Message: " + e.getMessage(), e);
        }

        return outputBuffer.toString();
    }

    public static Option generateOption(@NotEmpty final String opt,
                                        @NotEmpty final String longOpt,
                                        final boolean hasArg,
                                        @NotEmpty final String description,
                                        final boolean required) {
        final Option option = new Option(opt, longOpt, hasArg, description);
        option.setRequired(required);
        return option;
    }

    public static CommandLine parseOptions(@NonNull final Options options,
                                           @NonNull final String[] args) throws ParseException {

        final CommandLine cmd;
        final CommandLineParser parser = new GnuParser();
        try {
            cmd = parser.parse(options, args);
        } catch (final ParseException e) {
            throw e;
        }
        return cmd;
    }

    public static void printHelp(@NonNull final Options options,
                            @NotEmpty final String cmdLineSyntax,
                            @NotEmpty final String header,
                            @NotEmpty final String footer) {
        final PrintWriter pw = IoBuilder.forLogger().setLevel(Level.ERROR).buildPrintWriter();
        final HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(pw, 120, cmdLineSyntax, header, options, 0, 0, footer, true);
    }
}
