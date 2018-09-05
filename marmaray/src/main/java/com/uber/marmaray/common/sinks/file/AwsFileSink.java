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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.DispersalType;
import com.uber.marmaray.common.configuration.AwsConfiguration;
import com.uber.marmaray.common.configuration.FileSinkConfiguration;
import com.uber.marmaray.common.configuration.HadoopConfiguration;
import com.uber.marmaray.common.converters.data.FileSinkDataConverter;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.spark_project.guava.annotations.VisibleForTesting;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

/**
 * {@link AwsFileSink} implements {@link FileSink} interface to build a FileSink
 * that first convert data to String with csv format
 * and then save to Aws bucket with config defined in {@link AwsConfiguration}
 */
@Slf4j
public class AwsFileSink extends FileSink {
    private static final String SUCCESS = "_SUCCESS";
    private static final String CRC = ".crc";
    protected final AmazonS3 s3Client;
    private final AwsConfiguration awsConf;

    public AwsFileSink(@NonNull final FileSinkConfiguration conf,
                       @NonNull final FileSinkDataConverter converter) {
        super(conf, converter);
        this.awsConf = new AwsConfiguration(conf);
        this.s3Client = getS3Connection();
    }

    /**
     * This method is used to initialize {@link AwsFileSink#s3Client}
     * with aws configurations in {@link AwsConfiguration}
     *
     * @return new AmazonS3 client
     */
    protected AmazonS3 getS3Connection() {
        final AWSStaticCredentialsProvider awsCredentialProvider
                = new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(this.awsConf.getAwsAccessKeyId(),
                                this.awsConf.getAwsSecretAccessKey()));
        return AmazonS3ClientBuilder.standard().withRegion(this.awsConf.getRegion())
                .withCredentials(awsCredentialProvider).build();
    }

    /**
     * This method is used to upload single file to aws s3.
     *
     * @param fileSystem file system of intermediate path
     * @param path  source file path
     * @param partNum partition number of the file
     */
    private void uploadFileToS3(@NonNull final FileSystem fileSystem,
                                @NonNull final Path path, @NonNull final int partNum) {
        byte[] contentBytes = new byte [0];
        log.info("Start upload file to S3 with partition num: {}", partNum);
        log.info("Start calculating file bytes.");
        try (final InputStream input = fileSystem.open(path)) {
            contentBytes = IOUtils.toByteArray(input);
        } catch (IOException e) {
            log.error("Failed while reading bytes from source path with message %s", e.getMessage());
            throw new JobRuntimeException(e);
        }
        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(contentBytes.length);
        log.info("Uploading from {} to S3 bucket {}/{}", path.toString()
                , this.awsConf.getBucketName(), this.awsConf.getObjectKey());
        try (final InputStream inputStream = fileSystem.open(path)) {
            final String objKey = String.format("%s_%0" + this.digitNum + "d", this.awsConf.getS3FilePrefix(), partNum);
            final PutObjectRequest request = new PutObjectRequest(this.awsConf.getBucketName(),
                    objKey, inputStream, metadata);
            this.s3Client.putObject(request);
        } catch (AmazonServiceException e) {
            log.error("Failed while putObject to bucket %s with message %s"
                    , this.awsConf.getBucketName(), e.getErrorMessage());
            throw new JobRuntimeException(e);
        } catch (IOException e) {
            log.error("Failed while open source path with %s", e.getMessage());
            throw new JobRuntimeException(e);
        }
    }

    /**
     * This method overrides {@link FileSink#write(JavaRDD)}
     * If the {@link FileSinkConfiguration#dispersalType} is OVERWRITE,
     * it will overwrite existing files with prefix {@link AwsConfiguration #objectKey} in {@link AwsConfiguration #bucketName}
     * Then save converted and repartitioned data to temporary path {@link FileSinkConfiguration#fullPath}
     * And finally upload each file in that path to aws s3 bucket with {@link AwsFileSink#uploadFileToS3(FileSystem, Path, int)}
     *
     * @param data data to upload to aws s3
     */
    @Override
    public void write(@NonNull final JavaRDD<AvroPayload> data) {
        //Delete temporary path for aws s3 if it exists.
        try {
            final Path destPath = new Path(this.awsConf.getSourcePath());
            final FileSystem fs =
                destPath.getFileSystem(new HadoopConfiguration(this.conf.getConf()).getHadoopConf());
            if (fs.exists(destPath)) {
                fs.delete(destPath, true);
            }
        } catch (IOException e) {
            log.error("Job failure while deleting temporary path {} before s3 sink write"
                    , this.awsConf.getSourcePath());
            throw new JobRuntimeException(e);
        }
        //Write data to temporary path
        super.write(data);
        final Path destPath = new Path(this.awsConf.getFileSystemPrefix());
        log.info("Start to load file system object for intermediate storage.");
        try {
            final FileSystem fileSystem =
                    destPath.getFileSystem(new HadoopConfiguration(this.conf.getConf()).getHadoopConf());

            //OVERWRITE Mode deletes all existing files in bucket with prefix: objectKey/partitionPath
            if (this.conf.getDispersalType() == DispersalType.OVERWRITE) {
                log.info("Start to overwrite files.");
                final ListObjectsRequest listObjectsRequest
                        = new ListObjectsRequest().withBucketName(this.awsConf.getBucketName())
                        .withPrefix(this.awsConf.getPathKey());
                final ArrayList<KeyVersion> keysToDelete = new ArrayList<>();
                ObjectListing objects = this.s3Client.listObjects(listObjectsRequest);
                int needDeleteNum = 0;
                do {
                    for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                        keysToDelete.add(new KeyVersion(objectSummary.getKey()));
                        needDeleteNum++;
                    }
                    objects = this.s3Client.listNextBatchOfObjects(objects);
                } while (objects.isTruncated());
                //Delete existing objects
                if (!keysToDelete.isEmpty()) {
                    final DeleteObjectsRequest multiObjectDeleteRequest =
                            new DeleteObjectsRequest(this.awsConf.getBucketName())
                                    .withKeys(keysToDelete)
                                    .withQuiet(false);
                    final DeleteObjectsResult delObjRes = this.s3Client.deleteObjects(multiObjectDeleteRequest);
                    final int successfulDeletes = delObjRes.getDeletedObjects().size();
                    // Verify that objects were deleted successfully.
                    log.info("Total number of overwritten files: {}", successfulDeletes);
                    if (successfulDeletes != needDeleteNum) {
                        final String errorMessage =
                                String.format("aws s3 client failed to delete objects, "
                                                + "expected num: %s, actual num: %s",
                                        needDeleteNum, successfulDeletes);
                        throw new JobRuntimeException(errorMessage);
                    }
                } else {
                    log.info("No files to overwrite in aws s3 bucket {} with prefix {}",
                            this.awsConf.getBucketName(), this.awsConf.getPathKey());
                }
            }
            //Both OVERWRITE Mode and VERSION Mode upload new file to S3 bucket
            int partitionId = 0;
            log.info("Start to collect file list.");
            final Path temporaryOutput = new Path(this.awsConf.getSourcePath());
            final FileStatus[] status = fileSystem.listStatus(temporaryOutput);
            log.info("Start to upload to S3 bucket.");
            for (final FileStatus s : status) {
                if (s.isFile()) {
                    final Path path = s.getPath();
                    final String fileName = path.getName();
                    if (!fileName.equals(SUCCESS) && !fileName.endsWith(CRC)) {
                        this.uploadFileToS3(fileSystem, path, partitionId);
                        partitionId += 1;
                    }
                }
            }
            log.info("Finished uploading to S3 bucket.");
            fileSystem.delete(temporaryOutput, true);
            log.info("Finished deleting temporary output path: {}", this.awsConf.getSourcePath());
        } catch (IOException e) {
            log.error("Failed Job while writing "
                    + "to s3 bucket %s with error message: %s", this.awsConf.getBucketName(), e.getMessage());
            throw new JobRuntimeException(e);
        }
    }

    @VisibleForTesting
    protected AmazonS3 getS3Client() {
        return this.s3Client;
    }
}
