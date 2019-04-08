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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.uber.marmaray.common.exceptions.MetadataException;

import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.JobMetrics;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import com.google.common.base.Optional;
import org.apache.parquet.Strings;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Set;

@Slf4j
/**
 * {@link MultiMetadataManager} implements the {@link IMetadataManager} interface.
 * It is a cassandra based metadata manager for HDFS files.
 */

public class MultiMetadataManager implements IMetadataManager<StringValue> {

    private final List<IMetadataManager<StringValue>> metadataManagersList;
    private Optional<Map<String, StringValue>> metadataMap = Optional.absent();

    @Getter
    private final AtomicBoolean shouldSaveChanges;

    /**
     * initialize metadata manager table
     */
    public MultiMetadataManager(@NonNull final List<IMetadataManager<StringValue>> metadataManagersList,
                              @NonNull final AtomicBoolean shouldSaveChanges) {

        this.metadataManagersList = ImmutableList.copyOf(metadataManagersList);
        this.shouldSaveChanges = shouldSaveChanges;
    }

    private Map<String, StringValue> getMetadataMap() {
        if (!this.metadataMap.isPresent()) {
            this.metadataMap = Optional.of(loadMetadata());
        }
        return this.metadataMap.get();
    }

    @Override
    public void setDataFeedMetrics(@NonNull final DataFeedMetrics dataFeedMetrics) {
        this.metadataManagersList.forEach(metadataManager ->
                        metadataManager.setDataFeedMetrics(dataFeedMetrics));
    }

    @Override
    public void setJobMetrics(@NonNull final JobMetrics jobMetrics) {
        this.metadataManagersList.forEach(metadataManager ->
                        metadataManager.setJobMetrics(jobMetrics));
    }

    /**
     * set operation applies on generated temporarily
     * map from Cassandra queries
     * @param key
     * @param value
     * @throws MetadataException
     */
    @Override
    public void set(@NotEmpty final String key, @NonNull final StringValue value) throws MetadataException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key));
        this.metadataManagersList.forEach(metadataManager -> metadataManager.set(key, value));
        getMetadataMap().put(key, value);
    }

    /**
     * remove operation on map
     * @param key
     * @return
     */
    @Override
    public Optional<StringValue> remove(@NotEmpty final String key) {
        this.metadataManagersList.forEach(metadataManager -> metadataManager.remove(key));
        return Optional.fromNullable(getMetadataMap().remove(key));
    }

    /**
     * get operation from map
     * @param key
     * @return
     * @throws MetadataException
     */
    @Override
    public Optional<StringValue> get(@NotEmpty final String key) throws MetadataException {
        return getMetadataMap().containsKey(key) ? Optional.of(getMetadataMap().get(key)) : Optional.absent();
    }

    /**
     * @return
     * Returns all keys
     */
    @Override
    public Set<String> getAllKeys() {
        return getMetadataMap().keySet();
    }

    /**
     * Upon a successful job, this method keeps the latest checkpoint.
     * @return
     * @throws IOException
     */
    @Override
    public void saveChanges() {
        if (this.shouldSaveChanges.compareAndSet(true, false)) {
            log.info("Saving checkpoint information.");
        } else {
            log.info("Checkpoint info is already saved. Not saving it again.");
            return;
        }

        /** update all children metadata managers with recent checkpoints*/
        getMetadataMap().forEach((key, value) -> {
                this.metadataManagersList.forEach(metadataManager -> metadataManager.set(key, value));
            });

        /** save checkpoints in all children metadata managers*/
        log.info("Save changes in all metadata managers");
        this.metadataManagersList.forEach(metadataManager -> {
                try {
                    metadataManager.saveChanges();
                } catch (IOException e) {
                    throw new MetadataException("Unable to save JobManager Metadata", e);
                }
            });
    }

    /**
     * load metadata map
     * @return latest metadata captured from all metadata managers
     */
    @VisibleForTesting
    private Map<String, StringValue> loadMetadata() {
        log.info("load all metadata managers");

        HashMap<String, StringValue> metadata = new HashMap<String, StringValue>();
        this.metadataManagersList.forEach(metadataManager -> {
                log.info("metadata manager : {}", metadataManager.toString());
                metadataManager.getAllKeys().forEach(key -> {
                        final Optional<StringValue> metadataManagerValue = metadataManager.get(key);
                        log.info("metadata: key: {}, value: {}", key, metadataManagerValue.get());
                        if (metadata.containsKey(key)) {
                            final StringValue currentValue = metadata.get(key);
                            int compareResult = metadataManagerValue.get().toString()
                                    .compareTo(currentValue.toString());
                            if (compareResult != 0) {
                                log.info("metadata mismatch in child manager!");
                                if (compareResult > 0) {
                                    metadata.put(key, metadataManagerValue.get());
                                }
                            }
                        } else {
                            metadata.put(key, metadataManagerValue.get());
                        }
                    });
            });
        return metadata;
    }
}
