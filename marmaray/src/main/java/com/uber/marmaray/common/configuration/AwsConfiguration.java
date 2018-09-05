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

import com.uber.marmaray.utilities.ConfigUtil;
import lombok.Getter;
import lombok.NonNull;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class AwsConfiguration implements Serializable {
    @Getter
    private final String region;
    @Getter
    private final String awsAccessKeyId;
    @Getter
    private final String awsSecretAccessKey;
    @Getter
    private final String bucketName;
    @Getter
    private final String objectKey;
    @Getter
    private final String s3FilePrefix;
    @Getter
    private final String sourcePath;
    @Getter
    private final String fileSystemPrefix;
    @Getter
    private final String pathKey;

    public AwsConfiguration(@NonNull final FileSinkConfiguration conf) {
        ConfigUtil.checkMandatoryProperties(conf.getConf(), this.getMandatoryProperties());
        this.region = conf.getAwsRegion().get();
        this.awsAccessKeyId = conf.getAwsAccessKeyId().get();
        this.awsSecretAccessKey = conf.getAwsSecretAccesskey().get();
        this.bucketName = conf.getBucketName().get();
        this.objectKey = conf.getObjectKey().get();
        this.sourcePath = conf.getFullPath();
        this.fileSystemPrefix = conf.getPathPrefix();

        String s3FilePrefix;
        if (conf.getSourcePartitionPath().isPresent()) {
            s3FilePrefix = String.format("%s/%s/", this.objectKey, conf.getSourcePartitionPath().get());
        } else {
            s3FilePrefix = String.format("%s/", this.objectKey);
        }
        this.pathKey = s3FilePrefix;
        s3FilePrefix += conf.getFileNamePrefix();
        this.s3FilePrefix = s3FilePrefix;
    }

    private List<String> getMandatoryProperties() {
        return Collections.unmodifiableList(
                Arrays.asList(
                        FileSinkConfiguration.AWS_REGION,
                        FileSinkConfiguration.BUCKET_NAME,
                        FileSinkConfiguration.OBJECT_KEY,
                        FileSinkConfiguration.AWS_ACCESS_KEY_ID,
                        FileSinkConfiguration.AWS_SECRET_ACCESS_KEY
                ));
    }
}
