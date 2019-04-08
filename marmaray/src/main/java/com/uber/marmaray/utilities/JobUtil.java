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
import com.uber.marmaray.common.status.IStatus;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

/**
 * {@link JobUtil} provides various utility functions during the execution of a job
 */
@Slf4j
public final class JobUtil {
    private JobUtil() {
        throw new JobRuntimeException("This utility class should not be instantiated");
    }

    // TODO: clean up datacenter configs
    public static String getDataCenterForJob(@NotEmpty final String dcPath) throws IOException {
        log.info("Looking up datacenter information in: {}", dcPath);
        final File dcFile = new File(dcPath);
        try (final FileInputStream fis = new FileInputStream(dcFile);
             final InputStream is = new BufferedInputStream(fis)) {
            final Scanner scanner = new Scanner(is);
            return scanner.next();
        }
    }

    public static void raiseExceptionIfStatusFailed(@NonNull final IStatus status) {
        if (IStatus.Status.FAILURE.equals(status.getStatus())) {
            if (status.getExceptions().isEmpty()) {
                throw new JobRuntimeException("Job has failed");
            } else {
                throw new JobRuntimeException(status.getExceptions().get(0));
            }
        }
    }
}
