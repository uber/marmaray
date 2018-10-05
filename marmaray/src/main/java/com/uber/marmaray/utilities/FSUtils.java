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

import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.HadoopConfiguration;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Stack;
import java.util.stream.IntStream;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * {@link FSUtils} defines utility methods with interacting with a filesystem
 */
@Slf4j
public class FSUtils {

    // Metadata file names in HDFS = nanoseconds since epoch so we can sort by name
    private static final Comparator<FileStatus> byTimestampedNameAsc =
            Comparator.comparingLong(f1 -> Long.parseLong(f1.getPath().getName()));

    /**
     * It returns FileSystem based on fs.defaultFS property defined in conf.
     */
    public static FileSystem getFs(final Configuration conf) throws IOException {
        return FileSystem.get((new HadoopConfiguration(conf)).getHadoopConf());
    }

    public static void deleteHDFSMetadataFiles(@NonNull final FileStatus[] fileStatuses,
                                            @NonNull final FileSystem fs,
                                            final int numFilesToRetain,
                                            final boolean fakeDelete) throws IOException {
        if (fileStatuses.length > numFilesToRetain) {
            Arrays.sort(fileStatuses, byTimestampedNameAsc);
            final int numToRemove = fileStatuses.length - numFilesToRetain;
            IntStream.range(0, numToRemove)
                    .forEach(i -> {
                            if (fileStatuses[i].isDirectory()) {
                                throw new RuntimeException("An unexpected directory was encountered. "
                                        + fileStatuses[i].getPath());
                            }

                            try {
                                if (fakeDelete) {
                                    log.info("{} would have been deleted", fileStatuses[i].getPath());
                                } else {
                                    log.info("Deleting {}", fileStatuses[i].getPath());
                                    fs.delete(fileStatuses[i].getPath(), false);
                                }
                            } catch (final IOException e) {
                                throw new RuntimeException("Unable to delete file: " + fileStatuses[i].getPath(), e);
                            }
                        });
        }
    }

    /**
     * Helper method to list files under "basePath". This needs to be used when you only need files which start after
     * "toProcessAfter" but ends before "toProcessBefore". Note that both the paths are exclusive.
     */
    public static Iterator<FileStatus> listFiles(@NonNull final FileSystem fs, @NonNull final Path basePath,
        @NonNull final Path toProcessAfter, @NonNull final Path toProcessBefore) throws IOException {
        return new Iterator<FileStatus>() {
            private final Stack<String> pathsToProcess = new Stack<>();
            private final Deque<FileStatus> newFilesQ = new LinkedList();
            private String lastProcessedFile = toProcessAfter.toUri().getRawPath();
            private final String pathToStopProcessingAfter = toProcessBefore.toUri().getRawPath();
            {
                Path currDir = toProcessAfter;
                if (!fs.isDirectory(currDir)) {
                    currDir = currDir.getParent();
                }
                final Stack<String> tmpFolders = new Stack<>();
                while (currDir != null && basePath.toUri().getRawPath().compareTo(currDir.toUri().getRawPath()) <= 0) {
                    tmpFolders.push(currDir.toUri().getRawPath());
                    currDir = currDir.getParent();
                }
                while (!tmpFolders.isEmpty()) {
                    this.pathsToProcess.push(tmpFolders.pop());
                }
            }

            // Helper method to recursively traverse folders. It aborts and returns as soon as newFilesQ becomes non
            // empty or we run out of folders to process
            private void computeNext() {
                while (!this.pathsToProcess.isEmpty() && this.newFilesQ.isEmpty()) {
                    final Path currentPath = new Path(this.pathsToProcess.pop());
                    try {
                        if (fs.isFile(currentPath)) {
                            this.lastProcessedFile = currentPath.toUri().getRawPath();
                            this.newFilesQ.add(fs.getFileStatus(currentPath));
                            break;
                        }
                        final FileStatus[] dirFiles = fs.listStatus(currentPath);
                        // Uses FileStatus's sorting based on name.
                        Arrays.sort(dirFiles);
                        final Stack<String> levelDirStack = new Stack<>();
                        for (final FileStatus nextFile : dirFiles) {
                            final String nextRawFilePath = nextFile.getPath().toUri().getRawPath();
                            if (nextRawFilePath.compareTo(this.lastProcessedFile) <= 0
                                || nextRawFilePath.compareTo(this.pathToStopProcessingAfter) >= 0) {
                                continue;
                            }
                            if (nextFile.isDirectory() || !levelDirStack.isEmpty()) {
                                levelDirStack.push(nextRawFilePath);
                                continue;
                            }
                            this.lastProcessedFile = nextRawFilePath;
                            this.newFilesQ.add(nextFile);
                        }
                        while (!levelDirStack.isEmpty()) {
                            this.pathsToProcess.push(levelDirStack.pop());
                        }
                    } catch (IOException e) {
                        throw new JobRuntimeException("error listing newFilesQ", e);
                    }
                }
            }

            @Override
            public boolean hasNext() {
                computeNext();
                return !this.newFilesQ.isEmpty();
            }

            @Override
            public FileStatus next() {
                computeNext();
                if (this.newFilesQ.isEmpty()) {
                    return null;
                } else {
                    return this.newFilesQ.pollFirst();
                }
            }
        };
    }
}
