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

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.exceptions.MetadataException;
import com.uber.marmaray.utilities.FSUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Strings;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link HDFSMetadataManager} implements the {@link IMetadataManager} interface, adding the capability
 * to put and retrieve generic metadata from HDFS.  All metadata will be stored under a single filename
 * with the name = System.currentTimeMillis() when {@link IMetadataManager#saveChanges()} is invoked
 */
@Slf4j
public class HDFSMetadataManager implements IMetadataManager<StringValue> {
    public static final int DEFAULT_NUM_METADATA_FILES_TO_RETAIN = 5;
    private static final int SERIALIZATION_VERSION = 1;
    private static final Comparator<FileStatus> byDateAsc =
        Comparator.comparingLong(f1 -> Long.parseLong(f1.getPath().getName()));

    // Using a thread-safe HashMap doesn't really provide any protection against jobs from other or same
    // customers running jobs against the same metadata directory.  We eventually want to take locks on
    // a directory (possivly via ZooKeeper) so only one job can operate at a time per job name.
    private final Map<String, StringValue> metadataMap;

    @NonNull
    private final FileSystem fileSystem;

    @NotEmpty @Getter
    private final String baseMetadataPath;

    /*
     *  If it is able to update {@link #shouldSaveChanges} from true to false; then only it will create new
     * metadata file and will save information in it.
     */
    @Getter
    private final AtomicBoolean shouldSaveChanges;

    public HDFSMetadataManager(@NonNull final FileSystem fs, @NotEmpty final String baseMetadataPath,
                               @NonNull final AtomicBoolean shouldSaveChanges)
            throws IOException {
        this.fileSystem = fs;
        this.baseMetadataPath = baseMetadataPath;
        this.shouldSaveChanges = shouldSaveChanges;
        this.metadataMap = loadMetadata();
    }

    @Override
    public void set(@NotEmpty final String key, @NonNull final StringValue value) throws MetadataException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key));
        this.metadataMap.put(key, value);
    }

    @Override
    public Optional<StringValue> remove(@NotEmpty final String key) {
        return Optional.fromNullable(this.metadataMap.remove(key));
    }

    @Override
    public Optional<StringValue> get(@NotEmpty final String key) throws MetadataException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key));
        return this.metadataMap.containsKey(key) ? Optional.of(this.metadataMap.get(key)) : Optional.absent();
    }

    @Override
    public Set<String> getAllKeys() {
        return this.metadataMap.keySet();
    }

    /**
     * This method will first attempt to save the metadata file to a temp file.  Once that succeeds it will
     * copy the file to it's permanent location without the temp extension.
     *
     * @return
     * @throws IOException
     */
    @Override
    public void saveChanges() {
        if (this.shouldSaveChanges.compareAndSet(true, false)) {
            log.info("Saving checkpoint information");
        } else {
            log.info("Checkpoint info is already saved. Not saving it again.");
            return;
        }

        final Callable<Void> callable = () -> {
            writeMetadataFile();
            pruneMetadataFiles();
            return null;
        };

        final Retryer<Void> retryer = RetryerBuilder.<Void>newBuilder()
            .retryIfExceptionOfType(Exception.class)
            .retryIfRuntimeException()
            .withWaitStrategy(WaitStrategies.exponentialWait(5, 20, TimeUnit.SECONDS))
            .withStopStrategy(StopStrategies.stopAfterAttempt(3))
            .build();

        try {
            retryer.call(callable);
        } catch (RetryException | ExecutionException e) {
            throw new JobRuntimeException("Could not save metadata file. " + e.getMessage(), e);
        }
    }

    private void pruneMetadataFiles() {
        try {
            final Path metadataPath = new Path(this.baseMetadataPath);
            if (this.fileSystem.exists(metadataPath)) {
                final FileStatus[] fileStatuses = fileSystem.listStatus(metadataPath);
                if (fileStatuses.length > 0) {
                    FSUtils.deleteHDFSMetadataFiles(fileStatuses,
                        this.fileSystem, DEFAULT_NUM_METADATA_FILES_TO_RETAIN, false);
                }
            }
        } catch (final IOException e) {
            final String errMsg =
                String.format("IOException occurred while pruning metadata files.  Message: %s", e.getMessage());
            log.warn(errMsg);
        }
    }

    private void writeMetadataFile() {
        final Long currentTime = System.currentTimeMillis();

        final String fileLocation = new Path(this.baseMetadataPath, currentTime.toString()).toString();
        final String tmpFileLocation = fileLocation.toString() + MetadataConstants.TEMP_FILE_EXTENSION;

        try (final OutputStream os = new BufferedOutputStream(
                this.fileSystem.create(
                    new Path(tmpFileLocation)))) {
            try (final ObjectOutputStream oos = new ObjectOutputStream(os)) {
                serialize(oos);
            }

            log.info("Saving metadata to: {}", fileLocation);
            this.fileSystem.rename(new Path(tmpFileLocation), new Path(fileLocation));
        } catch (final IOException e) {
            final String errMsg =
                String.format("IOException occurred while saving changes.  Message: %s", e.getMessage());
            throw new MetadataException(errMsg, e);
        }
    }

    public Optional<FileStatus> getLatestMetadataFile() throws IOException {
        // find the file name by timestamp and get the checkpoint metadata inside
        final Path parentFolder = new Path(this.baseMetadataPath);

        if (this.fileSystem.exists(parentFolder)) {
            // Get the latest metadata file written
            final java.util.Optional<FileStatus> fs = Arrays.stream(this.fileSystem.globStatus(
                    new Path(parentFolder, "*")))
                .filter(f -> !f.getPath().getName().endsWith(MetadataConstants.TEMP_FILE_EXTENSION))
                .sorted(byDateAsc.reversed()).findFirst();

            // Deserialize the map and load the checkpoint data
            return fs.isPresent() ? Optional.of(fs.get()) : Optional.absent();
        }
        return Optional.absent();
    }

    /**
     * This method will load the latest metadata file within the base metadata path for the
     * stated metadataKey.
     *
     * @return Map of Metadata keys to values
     * @throws IOException
     */
    public Map<String, StringValue> loadMetadata() throws IOException {
        log.info("Attempting to load metadata");
        final Optional<FileStatus> fs = getLatestMetadataFile();
        if (fs.isPresent()) {
            log.info("Loading metadata from: {}", fs.get().getPath());
            return loadMetadata(fs.get().getPath());
        } else {
            log.info("No metadata file found");
        }
        return new HashMap<String, StringValue>();
    }

    @VisibleForTesting
    /**
     * This method assumes that the path points explicitly to a metadata file and is not a directory
     * @param path
     * @return Map<String, StringValue>
     * @throws IOException
     */
    public Map<String, StringValue> loadMetadata(final Path path) throws IOException {
        try (final InputStream is = new BufferedInputStream(this.fileSystem.open(path))) {
            try (final ObjectInputStream input = new ObjectInputStream(is)) {
                return deserialize(input);
            }
        }
    }

    private void serialize(final ObjectOutputStream out) throws IOException {
        out.writeInt(SERIALIZATION_VERSION);
        out.writeInt(this.metadataMap.size());
        for (final Map.Entry<String, StringValue> entry : this.metadataMap.entrySet())  {
            log.info("Serializing key: {} and value: {}", entry.getKey(), entry.getValue().getValue());
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue().getValue());
        }

    }

    public static Map<String, StringValue> deserialize(final ObjectInputStream ois) throws IOException {
        final int version = ois.readInt();

        if (version == SERIALIZATION_VERSION) {
            final Map<String, StringValue> map = new HashMap<>();
            final int numEntries = ois.readInt();

            for (int i = 0; i < numEntries; i++) {
                final String key = ois.readUTF();
                final StringValue value = new StringValue(ois.readUTF());
                log.info("Deserializing key: {} and value: {}", key, value.getValue());
                map.put(key, value);
            }

            if (ois.available() > 0) {
                throw new MetadataException("Deserialization error, not all bytes were read off the stream");
            }

            return map;
        } else {
            throw new MetadataException("Version: " + version + " is not supported");
        }
    }

}
