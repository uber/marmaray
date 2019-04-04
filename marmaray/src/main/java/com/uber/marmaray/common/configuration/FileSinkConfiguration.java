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

import com.uber.marmaray.common.DispersalType;
import com.uber.marmaray.common.FileSinkType;
import com.uber.marmaray.common.PartitionType;
import com.uber.marmaray.common.exceptions.MissingPropertyException;
import com.uber.marmaray.common.DispersalLengthType;
import com.uber.marmaray.utilities.ConfigUtil;
import com.uber.marmaray.utilities.SchemaUtil;
import lombok.Getter;
import lombok.NonNull;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class FileSinkConfiguration implements Serializable {
    public static final String FILE_PREFIX_ONLY = "file.";
    public static final String FILE_COMM_PREFIX = Configuration.MARMARAY_PREFIX + FILE_PREFIX_ONLY;
    public static final String FILE_NAME_PREFIX = "marmaray";
    public static final String FS_PATH = FILE_COMM_PREFIX + "file_path";
    public static final String DEFAULT_FS_PATH = "/dispersal_output";
    public static final String PATH_PREFIX = FILE_COMM_PREFIX + "path_prefix";
    public static final String FILE_TYPE = FILE_COMM_PREFIX + "file_type";
    public static final String DEFAULT_FILE_TYPE = "csv";
    public static final String COMPRESSION = FILE_COMM_PREFIX + "compression";
    public static final Boolean DEFAULT_COMPRESSION = false;
    public static final String COMPRESSION_CODEC = FILE_COMM_PREFIX + "compression_codec";
    public static final String DEFAULT_COMPRESSION_CODEC = "lz4";
    public static final String ROW_IDENTIFIER = FILE_COMM_PREFIX + "row_identifier";
    public static final String DEFAULT_ROW_IDENTIFIER = "";
    public static final String CSV_COLUMN_HEADER = FILE_COMM_PREFIX + "with_column_header";
    public static final Boolean DEFAULT_CSV_COLUMN_HEADER = false;
    public static final String DISPERSAL_LENGTH = FILE_COMM_PREFIX + "dispersal_length";
    //Expected file size in MegaByte
    public static final String FILE_SIZE_MEGABYTE = FILE_COMM_PREFIX + "file_size_megabyte";
    //Default file size set output to one file.
    public static final long DEFAULT_FILE_SIZE = -1;
    public static final String SEPARATOR = FILE_COMM_PREFIX + "separator";
    public static final char DEFAULT_SEPARATOR = ',';
    //File name related setting
    public static final String PARTITION_TYPE = FILE_COMM_PREFIX + "partition_type";
    public static final PartitionType DEFAULT_PARTITION_TYPE = PartitionType.NONE;
    public static final String TIMESTAMP = FILE_COMM_PREFIX + SchemaUtil.DISPERSAL_TIMESTAMP;
    public static final String SOURCE_TYPE = FILE_COMM_PREFIX + "source_type";
    public static final String SOURCE_NAME_PREFIX = FILE_COMM_PREFIX + "source_name_prefix";
    public static final String SOURCE_PARTITION_PATH = FILE_COMM_PREFIX + "source_partition_path";
    public static final String DISPERSAL_TYPE = FILE_COMM_PREFIX + "dispersal_type";
    public static final DispersalType DEFAULT_DISPERSAL_TYPE = DispersalType.VERSION;
    //aws s3 properties names
    public static final String FILE_SINK_TYPE = FILE_COMM_PREFIX + "file_sink_type";
    public static final FileSinkType DEFAULT_FILE_SINK_TYPE = FileSinkType.valueOf("HDFS");
    public static final String AWS_REGION = FILE_COMM_PREFIX + "aws_region";
    public static final String BUCKET_NAME = FILE_COMM_PREFIX + "bucket_name";
    public static final String OBJECT_KEY = FILE_COMM_PREFIX + "object_key";
    public static final String AWS_ACCESS_KEY_ID = FILE_COMM_PREFIX + "aws_access_key_id";
    public static final String AWS_SECRET_ACCESS_KEY = FILE_COMM_PREFIX + "aws_secret_access_key";
    public static final String AWS_LOCAL = FILE_COMM_PREFIX + "aws_local";
    public static final String DEFAULT_AWS_LOCAL = "/aws_local_tmp";
    public static final String AWS_JOB_PREFIX = FILE_COMM_PREFIX + "aws_job_prefix";

    @Getter
    private final char separator;
    @Getter
    private final String path;
    @Getter
    private final String pathPrefix;
    @Getter
    private final String fullPath;
    @Getter
    private final String fileType;
    @Getter
    private final boolean compression;
    @Getter
    private final String compressionCodec;
    @Getter
    private final double fileSizeMegaBytes;
    @Getter
    private final boolean columnHeader;
    @Getter
    private final FileSinkType sinkType;

    //File name related setting
    @Getter
    private final String sourceType;
    @Getter
    private final String writeTimestamp;
    @Getter
    private final Optional<String> sourcePartitionPath;
    @Getter
    private final DispersalType dispersalType;
    @Getter
    private final String sourceNamePrefix;
    @Getter
    private final String pathHdfs;
    @Getter
    private final String fileNamePrefix;
    @Getter
    private final PartitionType partitionType;
    @Getter
    private final String rowIdentifier;

    //aws s3 properties
    @Getter
    private final Optional<String> awsRegion;
    @Getter
    private final Optional<String> bucketName;
    @Getter
    private final Optional<String> objectKey;
    @Getter
    private final Optional<String> awsAccessKeyId;
    @Getter
    private final Optional<String> awsSecretAccesskey;
    @Getter
    private final String awsLocal;
    @Getter
    private final Configuration conf;

    @Getter
    private final DispersalLengthType dispersalLength;

    public FileSinkConfiguration(@NonNull final Configuration conf)
            throws MissingPropertyException, UnsupportedOperationException {
        this.conf = conf;
        ConfigUtil.checkMandatoryProperties(this.conf, this.getMandatoryProperties());
        this.path = this.conf.getProperty(FS_PATH, DEFAULT_FS_PATH);
        this.fileType = this.conf.getProperty(FILE_TYPE, DEFAULT_FILE_TYPE);
        this.compression = this.conf.getBooleanProperty(COMPRESSION, DEFAULT_COMPRESSION);
        this.compressionCodec = this.conf.getProperty(COMPRESSION_CODEC, DEFAULT_COMPRESSION_CODEC);
        this.rowIdentifier = this.conf.getProperty(ROW_IDENTIFIER, DEFAULT_ROW_IDENTIFIER);
        this.fileSizeMegaBytes = this.conf.getDoubleProperty(FILE_SIZE_MEGABYTE, DEFAULT_FILE_SIZE);
        this.columnHeader = this.conf.getBooleanProperty(CSV_COLUMN_HEADER, DEFAULT_CSV_COLUMN_HEADER);

        //File System Prefix
        this.pathPrefix = this.conf.getProperty(PATH_PREFIX).get();

        if (this.conf.getProperty(SEPARATOR).isPresent()) {
            if (this.conf.getProperty(SEPARATOR).get().length() != 1) {
                throw new UnsupportedOperationException("The separator should only contain one single character.");
            } else {
                this.separator = this.conf.getProperty(SEPARATOR).get().charAt(0);
            }
        } else {
            this.separator = DEFAULT_SEPARATOR;
        }

        //File Partition Type
        if (this.conf.getProperty(PARTITION_TYPE).isPresent()) {
            this.partitionType = PartitionType.valueOf(this.conf.getProperty(PARTITION_TYPE)
                    .get().trim().toUpperCase());
        } else {
            this.partitionType = DEFAULT_PARTITION_TYPE;
        }

        //Data Dispersal Type: OverWrite or Version[default]
        if (this.conf.getProperty(DISPERSAL_TYPE).isPresent()) {
            final String dispersalType = this.conf.getProperty(DISPERSAL_TYPE).get().trim().toUpperCase();
            final Boolean isValid = EnumUtils.isValidEnum(DispersalType.class, dispersalType);
            if (isValid) {
                this.dispersalType = DispersalType.valueOf(dispersalType);
            } else {
                final String errorMessage
                        = String.format("The data dispersal type: %s is not supported.", dispersalType);
                throw new UnsupportedOperationException(errorMessage);
            }
        } else {
            this.dispersalType = DEFAULT_DISPERSAL_TYPE;
        }

        //File Sink Type : HDFS(default) or AWS S3
        if (this.conf.getProperty(FILE_SINK_TYPE).isPresent()) {
            final String sinkName = this.conf.getProperty(FILE_SINK_TYPE).get().trim().toUpperCase();
            final Boolean isValid = EnumUtils.isValidEnum(FileSinkType.class, sinkName);
            if (isValid) {
                this.sinkType = FileSinkType.valueOf(sinkName);
            } else {
                final String errorMessage = String.format("The file sink type: %s is not supported.", sinkName);
                throw new UnsupportedOperationException(errorMessage);
            }
        } else {
            this.sinkType = DEFAULT_FILE_SINK_TYPE;
        }

        //File Name and Path Configurations
        this.sourceNamePrefix = this.conf.getProperty(SOURCE_NAME_PREFIX).get();

        if (this.partitionType != PartitionType.NONE) {
            if (!this.conf.getProperty(SOURCE_PARTITION_PATH).isPresent()) {
                throw new MissingPropertyException(
                        "The source partition path is missing while partition type is not None.");
            }
            this.sourcePartitionPath = this.conf.getProperty(SOURCE_PARTITION_PATH);
        } else {
            if (this.conf.getProperty(HiveConfiguration.PARTITION_KEY_NAME).isPresent()) {
                throw new UnsupportedOperationException(
                        "The partition type is none and there shouldn't be partition key name in Hive Configuration.");
            }
            this.sourcePartitionPath = Optional.absent();
        }

        this.writeTimestamp = this.conf.getProperty(TIMESTAMP).get();
        this.sourceType = this.conf.getProperty(SOURCE_TYPE).get();

        if (this.conf.getProperty(DISPERSAL_LENGTH).isPresent()
                && this.conf.getProperty(DISPERSAL_LENGTH).get().equals("multiple_day")) {
            this.dispersalLength = DispersalLengthType.MULTIPLE_DAY;
        } else {
            this.dispersalLength = DispersalLengthType.SINGLE_DAY;
        }

        this.fileNamePrefix = String.format("%s_%s_%s_%s",
                FILE_NAME_PREFIX, this.sourceType, this.sourceNamePrefix, this.writeTimestamp);

        //Aws S3 configuration initialization
        this.awsRegion = this.conf.getProperty(AWS_REGION);
        this.bucketName = this.conf.getProperty(BUCKET_NAME);
        this.objectKey = this.conf.getProperty(OBJECT_KEY);
        this.awsAccessKeyId = this.conf.getProperty(AWS_ACCESS_KEY_ID);
        this.awsSecretAccesskey = this.conf.getProperty(AWS_SECRET_ACCESS_KEY);
        this.awsLocal = this.conf.getProperty(AWS_LOCAL, DEFAULT_AWS_LOCAL);
        String fullPath = StringUtils.EMPTY;
        if (this.sinkType == FileSinkType.HDFS) {
            fullPath = String.format("%s%s", this.pathPrefix, this.path);
            if (this.sourcePartitionPath.isPresent()) {
                fullPath += String.format("/%s", this.sourcePartitionPath.get());
            }
            this.fullPath = String.format("%s/%s", fullPath, this.fileNamePrefix);
        } else {
            this.fullPath = String.format("%s%s", this.pathPrefix, this.awsLocal);
        }
        this.pathHdfs = fullPath;
    }

    private List<String> getMandatoryProperties() {
        return Collections.unmodifiableList(
                Arrays.asList(
                        FileSinkConfiguration.SOURCE_TYPE,
                        FileSinkConfiguration.TIMESTAMP,
                        FileSinkConfiguration.PATH_PREFIX,
                        FileSinkConfiguration.SOURCE_NAME_PREFIX
                ));
    }
}
