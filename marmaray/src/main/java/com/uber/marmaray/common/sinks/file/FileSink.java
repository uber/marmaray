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

package com.uber.marmaray.common.sinks.file;

import com.google.common.base.Optional;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.FileSinkConfiguration;
import com.uber.marmaray.common.converters.data.FileSinkDataConverter;
import com.uber.marmaray.common.data.RDDWrapper;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.metrics.DataFeedMetricNames;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.common.sinks.ISink;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link FileSink} implements the {@link ISink} interface for a File sink.
 * {@link FileSinkDataConverter} converts data from AvroPayload type to String with csv format
 * The transformed data is repartitioned as desired and dispersal to required destination path
 */
@Slf4j
public abstract class FileSink implements ISink, Serializable {
    private static final long ROW_SAMPLING_THRESHOLD = 100;
    private static final int OUTPUT_ONE_FILE_CONFIG = -1;
    private static final int DEFAULT_DIGIT_NUM = 5;
    private static final int MIN_PARTITION_NUM = 1;
    private static final String SINK_INFO_TAG = "file_sink";
    protected final FileSinkConfiguration conf;
    protected final FileSinkDataConverter converter;
    protected Optional<DataFeedMetrics> dataFeedMetrics = Optional.absent();
    protected int digitNum;

    public FileSink(@NonNull final FileSinkConfiguration conf, @NonNull final FileSinkDataConverter converter) {
        this.conf = conf;
        this.converter = converter;
    }

    @Override
    public void setDataFeedMetrics(@NonNull final DataFeedMetrics dataFeedMetrics) {
        this.dataFeedMetrics = Optional.of(dataFeedMetrics);

    }

    @Override
    public void setJobMetrics(@NonNull final JobMetrics jobMetrics) {
        // ignored for now
    }

    /**
     * This method overrides write method in {@link ISink} and write data to FileSink
     * Data first converted by {@link FileSinkDataConverter}
     * then repartition by {@link FileSink#getRepartitionNum(JavaRDD)}
     * Finally saved in destination {@link FileSinkConfiguration#fullPath}
     *
     * @param data data to write to sink
     */
    @Override
    public void write(@NonNull final JavaRDD<AvroPayload> data) {
        final JavaRDD<String> convertedData = this.converter.convertAll(data);
        final int partNum = getRepartitionNum(convertedData);
        final int desiredDigit = (int) Math.floor(Math.log10(partNum) + 1);
        this.digitNum = desiredDigit > DEFAULT_DIGIT_NUM ? desiredDigit : DEFAULT_DIGIT_NUM;
        final JavaRDD<String> dataRepartitioned = convertedData.repartition(partNum);
        final JavaRDD<String> dataToWrite;
        if (this.conf.isColumnHeader()) {
            final String header = this.converter.getHeader(data);
            dataToWrite = addColumnHeader(header, dataRepartitioned);
        } else {
            dataToWrite = dataRepartitioned;
        }
        if (dataFeedMetrics.isPresent()) {
            final Map<String, String> tags = new HashMap<>();
            tags.put(SINK_INFO_TAG, this.conf.getSinkType().name());
            final RDDWrapper<String> dataWrapper = new RDDWrapper<>(dataToWrite);
            final long totalRows = dataWrapper.getCount();
            this.dataFeedMetrics.get().createLongMetric(DataFeedMetricNames.OUTPUT_ROWCOUNT,
                    totalRows, tags);
        }
        log.info("Start write {} to path {}", this.conf.getFileType(), this.conf.getFullPath());
        dataToWrite.saveAsTextFile(this.conf.getFullPath());
        log.info("Finished save to path: {}.", this.conf.getFullPath());
    }

    /**
     * This method add column header to the first line of each partition in @param data.
     * @param header column header
     * @param data data with partitions to add column header
     * @return new JavaRDD<String> with column header
     */
    protected JavaRDD<String> addColumnHeader(@NonNull final String header, @NonNull final JavaRDD<String> data) {
        final JavaRDD<String> result = data.mapPartitions((lines) -> {
                final List<String> partitionList = IteratorUtils.toList(lines);
                partitionList.add(0, header);
                return partitionList.iterator();
            });
        return result;
    }

    /**
     * This method repartition data based on {@link FileSinkConfiguration#fileSizeMegaBytes}
     * 1) {@link FileSinkConfiguration#fileSizeMegaBytes} = OUTPUT_ONE_FILE_CONFIG
     * => repartition number is 1
     * 2) {@link FileSinkConfiguration#fileSizeMegaBytes} != OUTPUT_ONE_FILE_CONFIG
     * => each output file with size in megabytes around {@link FileSinkConfiguration#fileSizeMegaBytes}
     * => repartition number = total data size in megabytes {@link FileSink#getRddSizeInMegaByte(JavaRDD)} / required megabytes per file
     *
     * @param data converted data to calculate repartition number
     * @return repartition number of data to save
     */
    protected int getRepartitionNum(@NonNull final JavaRDD<String> data) {
        final int parNum;
        if (this.conf.getFileSizeMegaBytes() == OUTPUT_ONE_FILE_CONFIG) {
            parNum = MIN_PARTITION_NUM;
        } else {
            final double rddSize = getRddSizeInMegaByte(data);
            log.info("Write data with megabytes: {}", rddSize);
            final int suggestedNum = (int) Math.round(rddSize / this.conf.getFileSizeMegaBytes());
            parNum = suggestedNum < MIN_PARTITION_NUM ? MIN_PARTITION_NUM : suggestedNum;
        }
        log.info("Write data with repartition number: {} ", parNum);
        return parNum;
    }

    /**
     * This method calculate data size in megabytes
     * If total row number > ROW_SAMPLE_THRESHOLD
     * => It samples data to row number = ROW_SAMPLE_THRESHOLD
     * => Calculate sample data size by {@link FileSink#getSampleSizeInBytes(JavaRDD)}
     * => Calculate total data sizes by fraction and change to megabyte
     * Else calculate total data size by {@link FileSink#getSampleSizeInBytes(JavaRDD)}
     *
     * @param data data to calculate size in megabytes
     * @return estimated data size in megabytes
     */
    protected double getRddSizeInMegaByte(@NonNull final JavaRDD<String> data) {
        final RDDWrapper<String> dataWrapper = new RDDWrapper<>(data);
        final long totalRows = dataWrapper.getCount();
        final double totalSize;
        if (totalRows > ROW_SAMPLING_THRESHOLD) {
            log.debug("Start sampling on Write Data.");
            final double fraction = (double) ROW_SAMPLING_THRESHOLD / (double) totalRows;
            log.debug("Sample fraction: {}", fraction);
            final JavaRDD<String> sampleRdd = data.sample(false, fraction);
            final long sampleSizeInBytes = getSampleSizeInBytes(sampleRdd);
            final double sampleSizeInMB = (double) sampleSizeInBytes / FileUtils.ONE_MB;
            totalSize = sampleSizeInMB / fraction;
        } else {
            totalSize = (double) getSampleSizeInBytes(data) / FileUtils.ONE_MB;
        }
        return totalSize;
    }

    /**
     * This method calculates size[bytes] of Strings in data
     *
     * @param data data to calculate size in bytes
     * @return sample size in bytes
     */
    protected long getSampleSizeInBytes(@NonNull final JavaRDD<String> data) {
        long size = 0;
        try {
            for (final String row: data.collect()) {
                log.debug("Line to calculate: {}", row);
                final byte[] utf8Bytes;
                utf8Bytes = row.getBytes("UTF-8");
                log.debug("Row Size: {}", utf8Bytes.length);
                //Add 1 byte size for end of line '/n' for each sample row
                size += utf8Bytes.length + 1;
            }
            log.debug("Sample size in bytes: {}", size);
        } catch (UnsupportedEncodingException e) {
            log.error("Failed while calculating sample size: {}", e.getMessage());
            throw new JobRuntimeException(e);
        }
        return size;
    }
}
