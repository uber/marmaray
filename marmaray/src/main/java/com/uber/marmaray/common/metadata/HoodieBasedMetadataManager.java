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
import com.uber.marmaray.common.configuration.HadoopConfiguration;
import com.uber.marmaray.common.configuration.HoodieConfiguration;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.utilities.FSUtils;
import com.uber.marmaray.utilities.HoodieUtil;
import com.uber.marmaray.utilities.MapUtil;
import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieAvroPayload;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.Strings;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * It should be used if metadata information needs to be stored in hoodie. It uses hoodie commit file to store
 * metadata information. In cases where where job has hoodie sink then it will have no-op for saveChanges().
 */
@Slf4j
public class HoodieBasedMetadataManager implements IMetadataManager<StringValue> {

    public static final String HOODIE_METADATA_KEY = "HoodieMetadataKey";

    @Getter
    private final HoodieConfiguration hoodieConf;
    private final AtomicBoolean saveChanges;
    private transient Optional<JavaSparkContext> jsc = Optional.absent();
    private final Map<String, String> metadataMap;

    /**
     * @param shouldSaveChanges {@link AtomicBoolean} which {@link #saveChanges} will use to determine if
     *                          it should create new commit and save changes or not. It will save changes into new commit only if CAS operation
     *                          succeeds in setting it to true (from false).
     * @param hoodieConf        {@link HoodieConfiguration}
     */
    public HoodieBasedMetadataManager(@NonNull final HoodieConfiguration hoodieConf,
        @NonNull final AtomicBoolean shouldSaveChanges, @NonNull final JavaSparkContext jsc) throws IOException {
        this.hoodieConf = hoodieConf;
        this.saveChanges = shouldSaveChanges;
        this.jsc = Optional.of(jsc);
        this.metadataMap = readMetadataInfo(this.hoodieConf);
    }

    /**
     * Updates in-memory metadata map with given key-value pair.
     */
    public void set(@NotEmpty final String key, @NonNull final StringValue value) {
        if (!this.saveChanges.get()) {
            throw new JobRuntimeException(
                String.format("Metadata manager changes are already saved.key:%s:value%s", key, value));
        }
        this.metadataMap.put(key, value.getValue());
    }

    /**
     * Remove the specified key from the metadata
     *
     * @param key the key to remove
     * @return Optional of value if it was present, Optional.absent() if not
     */
    @Override
    public Optional<StringValue> remove(@NotEmpty final String key) {
        final String val = this.metadataMap.remove(key);
        return val == null ? Optional.absent() : Optional.of(new StringValue(val));
    }

    /**
     * Returns given metadata key.
     */
    public Optional<StringValue> get(@NotEmpty final String key) {
        final String val = this.metadataMap.get(key);
        return val == null ? Optional.absent() : Optional.of(new StringValue(val));
    }

    /**
     * Returns all metadata manager keys.
     */
    @Override
    public Set<String> getAllKeys() {
        return this.metadataMap.keySet();
    }

    /**
     * Returns immutable map of metadata key-value pairs.
     */
    public Map<String, String> getAll() {
        return Collections.unmodifiableMap(this.metadataMap);
    }

    public AtomicBoolean shouldSaveChanges() {
        return this.saveChanges;
    }

    /**
     * If it is able to update {@link #saveChanges} from true to false; then only it will create new hoodie
     * commit and will save metadata information in it.
     */
    public void saveChanges() {
        if (!this.saveChanges.compareAndSet(true, false)) {
            log.info("Metadata info is already saved. Not saving it again.");
            return;
        }
        final HoodieWriteClient<HoodieAvroPayload> writeClient =
            new HoodieWriteClient<>(jsc.get(), this.hoodieConf.getHoodieWriteConfig(), true);
        final String commitTime = writeClient.startCommit();
        log.info("Saving metadata info using hoodie-commit: {}", commitTime);
        final List<WriteStatus> dummyWrites = new ArrayList<>();
        final boolean ret =
            writeClient
            .commit(commitTime, jsc.get().parallelize(dummyWrites), java.util.Optional.of(getMetadataInfo()));
        if (!ret) {
            throw new JobRuntimeException("Failed to save metadata information.");
        }
    }

    /**
     * This method will also be used by HoodieSink to retrieve and store metadata information.
     * It returns {@link HashMap<String, String>} with hoodie metadata information to be saved into commit file.
     * It returns {@link HashMap} instead of {@link Map} because hoodie needs it that way. Checkout
     * {@link HoodieWriteClient#commit(String, JavaRDD, java.util.Optional)} for more info.
     */
    public HashMap<String, String> getMetadataInfo() {
        final HashMap<String, String> map = new HashMap<>();
        map.put(HOODIE_METADATA_KEY, MapUtil.serializeMap(this.metadataMap));
        return map;
    }

    /*
     * It reads metadata from latest hoodie commit file. Hoodie metadata info is stored in commit file using
     * {@link #HOODIE_METADATA_KEY} key.
     */
    private static Map<String, String> readMetadataInfo(
            @NonNull final HoodieConfiguration hoodieConf) {
        try {
            final FileSystem fs = FSUtils.getFs(hoodieConf.getConf());
            HoodieUtil.initHoodieDataset(fs, hoodieConf);
            final HoodieTableMetaClient hoodieTableMetaClient =
                new HoodieTableMetaClient(new HadoopConfiguration(hoodieConf.getConf()).getHadoopConf(),
                    hoodieConf.getBasePath(), true);
            final HoodieActiveTimeline hoodieActiveTimeline = hoodieTableMetaClient.getActiveTimeline();
            final java.util.Optional<HoodieInstant> lastInstant = hoodieActiveTimeline.getCommitTimeline()
                .filterCompletedInstants().lastInstant();
            if (lastInstant.isPresent()) {
                log.info("using hoodie instant for reading checkpoint info :{}", lastInstant.get().getTimestamp());
                final HoodieCommitMetadata commitMetadata =
                    HoodieCommitMetadata.fromBytes(hoodieActiveTimeline.getInstantDetails(lastInstant.get()).get());
                final String serCommitInfo = commitMetadata.getMetadata(HOODIE_METADATA_KEY);
                if (!Strings.isNullOrEmpty(serCommitInfo)) {
                    return MapUtil.deserializeMap(serCommitInfo);
                }
            }
            return new HashMap<>();
        } catch (IOException e) {
            log.error("failed to read metadata info", e);
            throw new JobRuntimeException("failed to read metadata information", e);
        }
    }
}
