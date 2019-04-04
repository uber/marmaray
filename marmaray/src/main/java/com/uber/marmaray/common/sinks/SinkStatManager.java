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
package com.uber.marmaray.common.sinks;

import com.google.common.base.Optional;
import com.uber.marmaray.common.metadata.IMetadataManager;
import com.uber.marmaray.common.metadata.StringValue;
import com.uber.marmaray.utilities.MapUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * Convenience class for managing {@link SinkStat} for {@link #MAX_HISTORY_SIZE} runs.
 */
@Slf4j
@AllArgsConstructor
@ToString
public class SinkStatManager {

    public static final int MAX_HISTORY_SIZE = 8;
    public static final String METAKEY = "sink-stat-%s";

    @Getter
    @NonNull
    private final String tableName;

    @NonNull
    private final IMetadataManager<StringValue> metadataManager;

    @Getter
    @NonNull
    private final SinkStat currentStat = new SinkStat();

    private final Queue<SinkStat> sinkStatQ = new LinkedList<>();

    public String getMetakey() {
        return String.format(METAKEY, tableName);
    }

    public void init() {
        final Optional<StringValue> serialisedStats = this.metadataManager.get(getMetakey());
        if (serialisedStats.isPresent()) {
            final Map<String, String> statHistory = MapUtil.deserializeMap(serialisedStats.get().getValue());
            for (int i = 0; i < statHistory.size(); i++) {
                this.sinkStatQ.add(SinkStat.deserialize(statHistory.get(Integer.toString(i))));
            }
        }
    }

    public void persist() {
        final Map<String, String> stats = new HashMap<>();
        if (!this.currentStat.isEmpty()) {
            this.sinkStatQ.add(this.currentStat);
        }
        while (this.sinkStatQ.size() > MAX_HISTORY_SIZE) {
            this.sinkStatQ.poll();
        }
        for (int i = 0; !this.sinkStatQ.isEmpty(); i++) {
            stats.put(Integer.toString(i), SinkStat.serialize(this.sinkStatQ.poll()));
        }
        this.metadataManager.set(getMetakey(), new StringValue(MapUtil.serializeMap(stats)));
    }

    public boolean isStatHistoryAvailable() {
        return !sinkStatQ.isEmpty();
    }

    public long getAvgRecordSize() {
        long avgRecordSize = 0;
        long numEntries = 0;
        for (final SinkStat stat : this.sinkStatQ) {
            final Optional<String> avgRecordSizeStat = stat.get(SinkStat.AVG_RECORD_SIZE);
            if (avgRecordSizeStat.isPresent()) {
                avgRecordSize += Long.parseLong(avgRecordSizeStat.get());
                numEntries += 1;
            }
        }
        log.info("tableName:{}:avgRecordSize:{}:numEntries:{}", this.tableName, avgRecordSize, numEntries);
        return avgRecordSize / Math.max(numEntries, 1);
    }

    @Slf4j
    @ToString
    public static class SinkStat {
        public static final String AVG_RECORD_SIZE = "AVG_RECORD_SIZE";

        private final Map<String, String> stats = new HashMap<>();

        public static SinkStat deserialize(@NonNull final String serializedStat) {
            final SinkStat sinkStat = new SinkStat();
            sinkStat.stats.putAll(MapUtil.deserializeMap(serializedStat));
            return sinkStat;
        }

        public static String serialize(@NonNull final SinkStat sinkStat) {
            return MapUtil.serializeMap(sinkStat.stats);
        }

        public Optional<String> get(@NotEmpty final String statKey) {
            final String statVal = this.stats.get(statKey);
            return statVal == null ? Optional.absent() : Optional.of(statVal);
        }

        public void put(@NotEmpty final String statKey, @NotEmpty final String statVal) {
            this.stats.put(statKey, statVal);
        }

        public boolean isEmpty() {
            return this.stats.isEmpty();
        }
    }
}
