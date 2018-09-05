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
package com.uber.marmaray.common.configuration;

import com.google.common.base.Optional;
import lombok.Getter;
import lombok.NonNull;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.Serializable;
import java.util.Date;

/**
 * {@link HiveSourceConfiguration} class contains all the metadata for running of data pipeline job where Hive
 * is the source of data. This class extends {@link HiveConfiguration}
 *
 * All properties start with {@link #HIVE_SOURCE_PREFIX}.
 */
public class HiveSourceConfiguration extends HiveConfiguration implements Serializable {

    public static final String HIVE_SOURCE_PREFIX = HIVE_PROPERTY_PREFIX + "source.";
    public static final String SAVE_CHECKPOINT = HIVE_SOURCE_PREFIX + "save_checkpoint";

    /**
     * Used for the very first run to determine first hive partition to disperse (if any).
     */
    public static final String HIVE_START_DATE = HIVE_SOURCE_PREFIX + "start_date";
    public static final String HIVE_START_DATE_FORMAT = "yyyy-MM-dd";

    @Getter
    private final Optional<Date> startDate;

    /**
     * This allows the option to reprocess an old partition without the need to write a new checkpoint if the
     * partition was processed in the past.
     */
    private final boolean saveCheckpoint;

    public HiveSourceConfiguration(@NonNull final Configuration conf) {
        super(conf);
        this.saveCheckpoint = this.getConf().getBooleanProperty(SAVE_CHECKPOINT, true);

        this.startDate = getConf().getProperty(HIVE_START_DATE).isPresent()
                ? Optional.of(DateTime.parse(getConf().getProperty(HIVE_START_DATE).get(),
                DateTimeFormat.forPattern(HIVE_START_DATE_FORMAT).withZoneUTC()).toDate())
                : Optional.absent();
    }

    public boolean shouldSaveCheckpoint() {
        return this.saveCheckpoint;
    }
}
