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
package com.uber.marmaray.common.converters.data;

import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.exceptions.InvalidDataException;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.marmaray.utilities.HoodieSinkConverterErrorExtractor;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotEmpty;

import static com.uber.marmaray.utilities.DateUtil.DATE_PARTITION_FORMAT;

/**
 * {@link TSBasedHoodieSinkDataConverter} extends {@link HoodieSinkDataConverter}
 *
 * This class generates {@link HoodieKey} from given {@link AvroPayload}. The passed in {@link AvroPayload} requires
 * {@link #partitionFieldName} with timestamp in {@link #timeUnit}.
 *
 * {@link AvroPayload} also requires a {@link #recordKeyFieldName} which should be the primary key for the record.
 */
@Slf4j
public class TSBasedHoodieSinkDataConverter extends HoodieSinkDataConverter {

    public static final SimpleDateFormat PARTITION_FORMATTER = new SimpleDateFormat(DATE_PARTITION_FORMAT);
    @NotEmpty
    private final String recordKeyFieldName;
    @NotEmpty
    private final String partitionFieldName;
    @NonNull
    private final TimeUnit timeUnit;

    public TSBasedHoodieSinkDataConverter(@NonNull final Configuration conf,
        @NotEmpty final String recordKeyFieldName, @NotEmpty final String partitionFieldName,
        @NonNull final TimeUnit timeUnit) {
        super(conf, new HoodieSinkConverterErrorExtractor());
        this.recordKeyFieldName = recordKeyFieldName;
        this.partitionFieldName = partitionFieldName;
        this.timeUnit = timeUnit;
    }

    @Override
    protected String getRecordKey(@NonNull final AvroPayload payload) throws Exception {
        final Object recordKeyFieldVal = payload.getField(recordKeyFieldName);
        if (recordKeyFieldVal == null) {
            throw new InvalidDataException("required field is missing:" + recordKeyFieldName);
        }
        return recordKeyFieldVal.toString();
    }

    @Override
    protected String getPartitionPath(final AvroPayload payload) throws Exception {
        final Object partitionFieldVal = payload.getField(partitionFieldName);
        if (partitionFieldVal == null) {
            throw new InvalidDataException("required field is missing:" + partitionFieldName);
        }
        final Date date = new Date(this.timeUnit.toMillis((long) Double.parseDouble(partitionFieldVal.toString())));
        return PARTITION_FORMATTER.format(date);
    }
}
