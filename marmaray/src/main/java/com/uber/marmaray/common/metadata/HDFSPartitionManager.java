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

package com.uber.marmaray.common.metadata;
import com.google.common.base.Optional;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.utilities.StringTypes;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * {@link HDFSPartitionManager} adds specific capabilities to
 * add, read, and retrieve partition information that is stored in HDFS.
 */
@Slf4j
public class HDFSPartitionManager {
    @NotEmpty @Getter
    protected final String rawDataRootPath;

    @NonNull
    protected final FileSystem fileSystem;

    @NotEmpty
    protected final String metadataKey;

    @Getter
    private final boolean isSinglePartition;

    public HDFSPartitionManager(@NotEmpty final String metadataKey,
                                @NotEmpty final String genericBaseMetadataPath,
                                @NotEmpty final String baseDataPath,
                                @NonNull final FileSystem fileSystem) throws IOException {
        this.metadataKey = metadataKey;
        this.rawDataRootPath = new Path(baseDataPath).toString();
        log.info(this.toString());
        this.fileSystem = fileSystem;
        try {
            final FileStatus[] fileStatuses = this.fileSystem.listStatus(new Path(baseDataPath));
            this.isSinglePartition = !Arrays.stream(fileStatuses).anyMatch(fs -> fs.isDirectory());
        } catch (final IOException e) {
            throw new JobRuntimeException("IOException encountered. Path:" + baseDataPath, e);
        }
    }

    /**
     * Our explicit assumption is that all partitions will be nested 1 level deep
     * under the file path under the metadata key.  There's no need at this time to have
     * more directories but that is subject to change in the future based on evolving requirements.
     * @return
     * @throws IOException
     */
    public Optional<String> getNextPartition(
            @NotEmpty final Optional<StringValue> latestCheckpoint) throws IOException {
        if (this.isSinglePartition) {
            log.info("Next partition: {}", this.rawDataRootPath);
            return Optional.of(this.rawDataRootPath);
        } else {
            if (latestCheckpoint.isPresent()) {
                log.info("Last checkpoint: {}", latestCheckpoint.get());
            } else {
                log.info("No last checkpoint found");
            }

            final java.util.Optional nextPartition = listPartitionsAfterCheckpoint(latestCheckpoint)
                    .stream()
                    .sorted()
                    .findFirst();

            if (nextPartition.isPresent()) {
                log.info("Next partition: {}", nextPartition.get());
                return Optional.of((String) nextPartition.get());
            } else {
                log.info("No partitions were found to process");
                return Optional.absent();
            }
        }
    }

    public List<String> getExistingPartitions() throws IOException {
        final String partitionFolderRegex = this.rawDataRootPath + File.separator + "*";
        log.info("Searching for partitions in path: {}", partitionFolderRegex);
        final FileStatus[] fileStatuses  = this.fileSystem.globStatus(new Path(partitionFolderRegex));
        final List<String> partitions = Arrays.asList(fileStatuses).stream()
                .map(fileStatus -> fileStatus.getPath().getName())
                // filter out hidden files/directories
                .filter(path -> !path.startsWith(StringTypes.DOT))
                .collect(Collectors.toList());
        return partitions;
    }

    private List<String> listPartitionsAfterCheckpoint(final Optional<StringValue> checkpoint) throws IOException {
        final List<String> partitions = getExistingPartitions();

        if (checkpoint.isPresent()) {
            return partitions.stream()
                    .filter(partition -> partition.compareTo(checkpoint.get().getValue()) > 0)
                    .collect(Collectors.toList());
        } else {
            return partitions;
        }
    }

    private String convertPartitionToPath(final String partition) {
        return new Path(this.rawDataRootPath, partition).toString();
    }
}
