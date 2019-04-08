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
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.utilities.DateUtil;
import com.uber.marmaray.utilities.StringTypes;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link HDFSDatePartitionManager} is an extension of {@link HDFSPartitionManager}
 * and provides functionality to specifically handle date partitions.
 * All date times should be in UTC time and in the format YYYY-MM-DD
 */
@Slf4j
public class HDFSDatePartitionManager extends HDFSPartitionManager {

    private static final String DEFAULT_START_DATE = "1970-01-01";
    private final String partitionKeyName;
    private final boolean hasPartitionKeyInHDFSPartitionPath;
    private final Optional<Date> startDate;

    public HDFSDatePartitionManager(@NotEmpty  final String metadataKey,
                                    @NotEmpty final String genericBaseDataPath,
                                    @NotEmpty final String partitionKeyName,
                                    @NonNull final Optional<Date> startDate,
                                    @NonNull final FileSystem fileSystem) throws IOException {
        super(metadataKey, genericBaseDataPath, fileSystem);
        this.partitionKeyName = partitionKeyName + StringTypes.EQUAL;
        this.hasPartitionKeyInHDFSPartitionPath = hasPartitionKeyNameInPartition();
        this.startDate = startDate;
        log.info("HDFSDatePartitionManager has partitionKey in HDFS path: {}", this.hasPartitionKeyInHDFSPartitionPath);
    }

    @Override
    public Optional<String> getNextPartition(
            @NotEmpty final Optional<StringValue> latestCheckPoint) throws IOException {

        if (this.isSinglePartition()) {
            log.info("Next partition: {}", this.rawDataRootPath);
            return Optional.of(this.rawDataRootPath);
        } else {
            if (latestCheckPoint.isPresent()) {
                log.info("Last checkpoint: {}", latestCheckPoint.get().getValue());
            } else {
                log.info("No last checkpoint found");
            }

            final LocalDate startDate = getDefaultStartDate();

            final Optional<LocalDate> dt = latestCheckPoint.isPresent()
                    ? Optional.of(DateUtil.convertToUTCDate(
                            latestCheckPoint.get().getValue().replace(this.partitionKeyName, StringTypes.EMPTY)))
                    : Optional.absent();

            final LocalDate compareDate = !dt.isPresent() || startDate.isAfter(dt.get()) ? startDate : dt.get();

            final List<LocalDate> existingPartitions = listSortedPartitionsAfterDate(compareDate);
            if (!existingPartitions.isEmpty()) {
                // get first partition after the checkpoint
                final String nextPartition = this.hasPartitionKeyInHDFSPartitionPath
                        ? this.partitionKeyName + existingPartitions.get(0).toString()
                        : existingPartitions.get(0).toString();

                log.info("Next partition to process: {}", nextPartition);
                return Optional.of(nextPartition);
            } else {
                log.info("No partitions found to be processed");
                return Optional.absent();
            }
        }
    }

    private LocalDate getDefaultStartDate() {
        final ZoneId UTC = ZoneId.of("Z");
        final LocalDate ld =  this.startDate.isPresent() ? this.startDate.get().toInstant().atZone(UTC).toLocalDate()
                : LocalDate.parse(DEFAULT_START_DATE);
        log.info("Default start date: {}", ld.toString());
        return ld;
    }
    /**
     * Returns the partitions in sorted ascending order which are after the date value
     * @param localDate
     * @return
     * @throws IOException
     */
    private List<LocalDate> listSortedPartitionsAfterDate(final LocalDate localDate) throws IOException {
        final LocalDate startDate = localDate.plusDays(1);

        final List<LocalDate> partitions = getExistingPartitions()
                .stream()
                .map(dt -> DateUtil.convertToUTCDate(dt.replace(this.partitionKeyName, StringTypes.EMPTY)))
                .filter(dt -> dt.compareTo(startDate) >= 0)
                .collect(Collectors.toList());

        return partitions;
    }

    private boolean hasPartitionKeyNameInPartition()  {
        try {
            final boolean hasPartitionKey = getExistingPartitions()
                    .stream()
                    .anyMatch(partition -> partition.startsWith(this.partitionKeyName));
            return hasPartitionKey;
        } catch (IOException e) {
            throw new JobRuntimeException(String.format("Unable to read existing partitions in the HDFS Path {}",
                    this.rawDataRootPath));
        }
    }
}
