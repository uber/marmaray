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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.exceptions.MetadataException;
import com.uber.marmaray.common.exceptions.MissingPropertyException;
import com.uber.marmaray.utilities.FSUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link JobManagerMetadataTracker} offers functionality to store JobDag's metadata information at
 * the Job Manager's level
 * The previous job's metadata is consumed at the start of each run and serialized after current execution
 * Uses {@link HDFSMetadataManager} internally to interact with the backend if sourceType is set to HDFS
 *
 */
@Slf4j
public class JobManagerMetadataTracker {

    @NonNull
    private IMetadataManager metadataManager;
    @Getter
    private AtomicBoolean shouldSaveChanges;

    private final ObjectMapper mapper = new ObjectMapper();
    private final TypeReference<Map<String, String>> typeRef = new TypeReference<Map<String, String>>() { };

    public JobManagerMetadataTracker(@NonNull final Configuration config) throws IOException {
        final Optional<String> sourceType = config.getProperty(MetadataConstants.JOBMANAGER_METADATA_STORAGE);
        if (!sourceType.isPresent()) {
            throw new MissingPropertyException("Source information for the JobManager Metadata Tracker is missing.");
        }
        if (sourceType.get().toUpperCase().equals(MetadataConstants.JOBMANAGER_METADATA_SOURCE_HDFS)) {
            final Optional<String> basePath = config.getProperty(MetadataConstants.JOBMANAGER_METADATA_HDFS_BASEPATH);
            if (!basePath.isPresent()) {
                throw new MissingPropertyException("Base Path for HDFS JobManager Metadata Tracker is missing.");
            }
            this.metadataManager =
                    new HDFSMetadataManager(FSUtils.getFs(config), basePath.get(), new AtomicBoolean(true));
            this.shouldSaveChanges = new AtomicBoolean(true);
        }
    }

    /**
     * Set the metadata for this DAG, if not empty
     * @param key
     * @param value
     */
    public void set(@NotEmpty final String key, @NonNull final Map<String, String> value) {
        try {
            if (!value.isEmpty()) {
                this.metadataManager.set(key, new StringValue(mapper.writeValueAsString(value)));
            }
        } catch (JsonProcessingException e) {
            throw new MetadataException("Unable to set the JobManager metadata for key :" + key);
        }
    }

    /***
     * Checks if metadata for a given DAG already exists
     * @param key
     * @return
     */
    public boolean contains(@NotEmpty final String key) {
        return this.metadataManager.get(key).isPresent() ? true : false;
    }

    /***
     * Returns the metadata for the given DAG
     * @param key
     * @return
     * @throws IOException
     */
    public Optional<Map<String, String>> get(@NotEmpty final String key) throws IOException {
        final Optional<StringValue> metadataValues =  this.metadataManager.get(key);
        if (metadataValues.isPresent()) {
            return Optional.of(mapper.readValue(metadataValues.get().getValue(), typeRef));
        }
        return Optional.absent();
    }

    /***
     * Invokes the {@link IMetadataManager} to store the metadata information
     *
     */
    public void writeJobManagerMetadata() {
        if (!this.shouldSaveChanges.get()) {
            throw new MetadataException("JobManager metadata can only be saved once.");
        } else {
            try {
                this.metadataManager.saveChanges();
                this.shouldSaveChanges.compareAndSet(true, false);
            } catch (IOException e) {
                throw new MetadataException("Unable to save JobManager Metadata", e);
            }
        }
    }
}
