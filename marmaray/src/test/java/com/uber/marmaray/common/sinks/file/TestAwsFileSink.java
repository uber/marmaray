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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.AwsConfiguration;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.FileSinkConfiguration;
import com.uber.marmaray.common.converters.data.FileSinkDataConverter;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.util.AvroPayloadUtil;
import com.uber.marmaray.utilities.ErrorExtractor;
import com.uber.marmaray.utilities.StringTypes;
import io.findify.s3mock.S3Mock;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Matchers;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

class MockAwsFileSink extends AwsFileSink {
    private static final String S3_TEST_URL = "http://localhost:8181";
    private static final String S3_TEST_BUCKET_NAME = "aws-test";
    private static final String S3_TEST_REGION = "us-east-1";
    private EndpointConfiguration endpoint;

    public MockAwsFileSink(@NonNull final FileSinkConfiguration conf,
    @NonNull final FileSinkDataConverter converter) throws IOException {
       super(conf, converter);
    }

    @Override
    protected AmazonS3 getS3Connection() {
        this.endpoint = new EndpointConfiguration(S3_TEST_URL, S3_TEST_REGION);
        AmazonS3 s3ClientTest = AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(endpoint)
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .build();
        s3ClientTest.createBucket(S3_TEST_BUCKET_NAME);
        return spy(s3ClientTest);
    }
}

@Slf4j
public class TestAwsFileSink extends FileSinkTestUtil {
    private static final String S3_TEST_URL = "http://localhost:8181";
    private static final String S3_TEST_REGION = "us-east-1";
    private static final String S3_TEST_BUCKET_NAME = "aws-test";
    private static final String S3_BUCKET_NAME = "uber-test";
    private static final String LOCAL1 = "/aws_test/test1";
    private static final String LOCAL2 = "/aws_test/test2";
    private static final String LOCAL3 = "/aws_test/test3";
    private static final String OBJ_KEY_1 = "test1";
    private static final String OBJ_KEY_2 = "test2";
    private static final String PARENT_DIR = "aws_test";
    private static final int NUM_RECORD = 100;
    private static final int EXPECTED_PARTITION_NUM = 2;
    private static final int EXPECTED_INVOCATIONS = 1;
    private static final String TIMESTAMP1 = "201808011025";
    private static final String TIMESTAMP2 = "201808012025";
    private static final String TIMESTAMP3 = "201808022025";
    private static final String SOURCE_SUB_PATH1 = "2018/08/01";
    private static final String SOURCE_SUB_PATH2 = "2018/08/02";
    private static final String VERSION = "version";
    private static final String OVERWRITE = "overwrite";
    private static final String S3_TEST_FOLDER = "s3-tests";
    private S3Mock s3mock;
    private String port;
    @Rule
    public TemporaryFolder s3mockRoot = new TemporaryFolder();

    @Before
    public void setupTest() {
        super.setupTest();
        try {
            port = S3_TEST_URL.substring(S3_TEST_URL.lastIndexOf(":") + 1);
            File s3mockDir = s3mockRoot.newFolder(S3_TEST_FOLDER);
            s3mock = S3Mock.create(Integer.parseInt(port), s3mockDir.getCanonicalPath());
            s3mock.start();
        } catch (IOException e) {
            log.error("Exception: {}", e.getMessage());
            throw new JobRuntimeException(e);
        }
    }

    @After
    public void tearDownTest() throws IOException {
        s3mock.stop();
        this.fileSystem.get().delete(new Path(PARENT_DIR), true);
        super.teardownTest();
    }

    /**Todo: T1984925- Fix the bug while running mock aws client.
    /**
     * The following two tests are used for test of uploading to existed aws s3 bucket.
     * If you need to test upload to a real aws s3 bucket, remove the ignore to test.
     * Also add credential file named credentials at test directory:usually marmaray/marmaray/credentials. It will be needed in
     * {@link FileSinkConfiguration} in {@link FileSinkTestUtil#initConfigWithAws}
     */

    /* Test bucket name: uber-test
     * Test object key: marmaray_test/test1/SOURCE_PARTITION_SUB_PATH/FILENAME_PREFIX_TIMESTAMP_PARTITION_NUM
     */
    @Ignore
    @Test
    public void testWriteToS3WithVersion() throws IOException {
        final Configuration conf1 = initConfigWithAws(this.pathPrefix, OBJ_KEY_1, LOCAL1, VERSION, TIMESTAMP1, SOURCE_SUB_PATH1, S3_BUCKET_NAME);
        final Configuration conf2 = initConfigWithAws(this.pathPrefix, OBJ_KEY_1, LOCAL2, VERSION, TIMESTAMP2, SOURCE_SUB_PATH1, S3_BUCKET_NAME);
        final Configuration conf3 = initConfigWithAws(this.pathPrefix, OBJ_KEY_1, LOCAL3, VERSION,  TIMESTAMP3, SOURCE_SUB_PATH2, S3_BUCKET_NAME);
        final JavaRDD<AvroPayload> testData = AvroPayloadUtil.generateTestData(this.jsc.get(),
                NUM_RECORD,
                StringTypes.EMPTY);
        testWriteGeneral(testData, conf1);
        testWriteGeneral(testData, conf2);
        testWriteGeneral(testData, conf3);
    }

    /* Test bucket name: uber-test
     * Test object key: marmaray_test/test2/SOURCE_PARTITION_SUB_PATH/FILENAME_PREFIX_TIMESTAMP_PARTITION_NUM
     */
    @Ignore
    @Test
    public void testWriteToS3WithSpecialCharAndOverWrite() throws IOException {
        final Configuration conf1 = initConfigWithAws(this.pathPrefix, OBJ_KEY_2, LOCAL1, VERSION, TIMESTAMP1, SOURCE_SUB_PATH1, S3_BUCKET_NAME);
        final Configuration conf2 = initConfigWithAws(this.pathPrefix, OBJ_KEY_2, LOCAL2, VERSION,  TIMESTAMP2, SOURCE_SUB_PATH1, S3_BUCKET_NAME);
        final Configuration conf3 = initConfigWithAws(this.pathPrefix, OBJ_KEY_2, LOCAL3, OVERWRITE,  TIMESTAMP3, SOURCE_SUB_PATH2, S3_BUCKET_NAME);
        final JavaRDD<AvroPayload> testData = AvroPayloadUtil.generateTestDataNew(this.jsc.get(),
                NUM_RECORD,
                StringTypes.EMPTY);
        testWriteGeneral(testData, conf1);
        testWriteGeneral(testData, conf2);
        testWriteGeneral(testData, conf3);
    }

    //The following tests are used for uploading to Mock S3 Client
    //Todo-T1984925: FIX test error here.
    @Ignore
    @Test
    public void testWriteToMockS3WithSingleVersion() throws IOException {
        final Configuration conf1 = initConfigWithAws(this.pathPrefix, OBJ_KEY_1, LOCAL1, VERSION,  TIMESTAMP1, SOURCE_SUB_PATH1, S3_TEST_BUCKET_NAME);
        testWriteToMockS3General(conf1);
    }
    @Ignore
    @Test
    public void testWriteToMockS3WithSingleOverwrite() throws IOException {
        final Configuration conf1 = initConfigWithAws(this.pathPrefix, OBJ_KEY_1, LOCAL1, OVERWRITE,  TIMESTAMP1, SOURCE_SUB_PATH1, S3_TEST_BUCKET_NAME);
        testWriteToMockS3General(conf1);
    }
    @Ignore
    @Test
    public void testWriteToMockS3WithMultiVersion() throws IOException {
        final Configuration conf1 = initConfigWithAws(this.pathPrefix, OBJ_KEY_1, LOCAL1, VERSION,  TIMESTAMP1, SOURCE_SUB_PATH1, S3_TEST_BUCKET_NAME);
        final Configuration conf2 = initConfigWithAws(this.pathPrefix, OBJ_KEY_1, LOCAL2, VERSION,  TIMESTAMP2, SOURCE_SUB_PATH1, S3_TEST_BUCKET_NAME);
        final Configuration conf3 = initConfigWithAws(this.pathPrefix, OBJ_KEY_1, LOCAL3, VERSION,  TIMESTAMP3, SOURCE_SUB_PATH2, S3_TEST_BUCKET_NAME);
        final List<String> filePrefix = new ArrayList<>();
        filePrefix.add(testWriteToMockS3General(conf1));
        filePrefix.add(testWriteToMockS3General(conf2));
        filePrefix.add(testWriteToMockS3General(conf3));
        final AmazonS3 MockClient = getMockS3Connection();
        assertTrue(MockClient.doesBucketExistV2(S3_TEST_BUCKET_NAME));
        for (String aFilePrefix : filePrefix) {
            for (int j = 0; j < EXPECTED_PARTITION_NUM; j++) {
                final Boolean objectExist = MockClient.doesObjectExist(S3_TEST_BUCKET_NAME, aFilePrefix + "_0000" + j);
                assertTrue(objectExist);
            }
        }
    }
    @Ignore
    @Test
    public void testWriteToMockS3WithMultiOverWrite() throws IOException {
        final Configuration conf1 = initConfigWithAws(this.pathPrefix, OBJ_KEY_1, LOCAL1, VERSION,  TIMESTAMP1, SOURCE_SUB_PATH1, S3_TEST_BUCKET_NAME);
        final Configuration conf2 = initConfigWithAws(this.pathPrefix, OBJ_KEY_1, LOCAL2, VERSION,  TIMESTAMP2, SOURCE_SUB_PATH2, S3_TEST_BUCKET_NAME);
        final Configuration conf3 = initConfigWithAws(this.pathPrefix, OBJ_KEY_1, LOCAL3, OVERWRITE,  TIMESTAMP3, SOURCE_SUB_PATH2, S3_TEST_BUCKET_NAME);
        final List<String> filePrefix = new ArrayList<String>();
        filePrefix.add(testWriteToMockS3General(conf1));
        filePrefix.add(testWriteToMockS3General(conf2));
        filePrefix.add(testWriteToMockS3General(conf3));
        final AmazonS3 MockClient = getMockS3Connection();
        assertTrue(MockClient.doesBucketExistV2(S3_TEST_BUCKET_NAME));
        for (int i = 0 ; i < filePrefix.size() ; i++) {
            for (int j = 0; j < EXPECTED_PARTITION_NUM; j++) {
                final Boolean objectExist = MockClient.doesObjectExist(S3_TEST_BUCKET_NAME, filePrefix.get(i) + "_0000" + j);
                if (i == 1) {
                    assertFalse(objectExist);
                } else {
                    assertTrue(objectExist);
                }
            }
        }

    }

    private String testWriteToMockS3General(@NonNull final Configuration conf) throws IOException {
        final JavaRDD<AvroPayload> testData = AvroPayloadUtil.generateTestDataNew(this.jsc.get(),
                NUM_RECORD,
                StringTypes.EMPTY);
        final FileSinkDataConverter converter = new FileSinkDataConverter(conf, new ErrorExtractor());
        final FileSinkConfiguration fileConf = new FileSinkConfiguration(conf);
        final AwsConfiguration awsConf = new AwsConfiguration(fileConf);
        final MockAwsFileSink awsMockSink = spy(new MockAwsFileSink(fileConf, converter));
        awsMockSink.write(testData);
        final AmazonS3 MockClient = awsMockSink.getS3Client();
        verify(awsMockSink, times(EXPECTED_INVOCATIONS)).write(Matchers.any(JavaRDD.class));
        verify(MockClient, times(EXPECTED_PARTITION_NUM)).putObject(Matchers.any(PutObjectRequest.class));
        assertTrue(MockClient.doesBucketExistV2(fileConf.getBucketName().get()));
        for (int i = 0 ; i < EXPECTED_PARTITION_NUM ; i++) {
            final Boolean objectExist = MockClient.doesObjectExist(fileConf.getBucketName().get(), awsConf.getS3FilePrefix()+ "_0000" + i);
            assertTrue(objectExist);
        }
        return awsConf.getS3FilePrefix();
    }

    private void testWriteGeneral(@NonNull final JavaRDD<AvroPayload> testData, @NonNull final Configuration conf) throws IOException {
        final FileSinkDataConverter converter = new FileSinkDataConverter(conf, new ErrorExtractor());
        final FileSinkConfiguration fileConf = new FileSinkConfiguration(conf);
        final FileSink awsSink = new AwsFileSink(fileConf, converter);
        awsSink.write(testData);
    }

    private AmazonS3 getMockS3Connection() {
        final EndpointConfiguration endpoint = new EndpointConfiguration(S3_TEST_URL, S3_TEST_REGION);
        AmazonS3 s3ClientTest = AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(endpoint)
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .build();
        return s3ClientTest;
    }
}
