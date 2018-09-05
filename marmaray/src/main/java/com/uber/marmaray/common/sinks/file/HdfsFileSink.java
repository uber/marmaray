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

import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.DispersalType;
import com.uber.marmaray.common.configuration.HadoopConfiguration;
import com.uber.marmaray.common.converters.data.FileSinkDataConverter;
import com.uber.marmaray.common.configuration.FileSinkConfiguration;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;

/**
 * {@link HdfsFileSink} implements {@link FileSink} interface to build a FileSink
 * that first convert data to String with csv format
 * and then save to Hdfs path defined in {@link FileSinkConfiguration#fullPath}
 */
@Slf4j
public class HdfsFileSink extends FileSink {
    private static final String SUCCESS = "_SUCCESS";
    private static final String CRC = ".crc";
    public HdfsFileSink(@NonNull final FileSinkConfiguration conf, @NonNull final FileSinkDataConverter converter) {
        super(conf, converter);
    }

    /**
     * This method overrides write method in {@link FileSink}
     * If the {@link FileSinkConfiguration#dispersalType} is OVERWRITE,
     * it will delete previous files in the destination path.
     * Then it will write new files to hdfs path: {@link FileSinkConfiguration #fullpath}
     * @param data data to write to hdfs file sink
     */
    @Override
    public void write(@NonNull final JavaRDD<AvroPayload> data) {
        if (this.conf.getDispersalType() == DispersalType.OVERWRITE) {
            log.info("Start to delete previous files.");
            log.debug("Full path: {}", this.conf.getPathHdfs());
            final Path dataFolder = new Path(this.conf.getPathHdfs());
            try {
                final FileSystem fileSystem =
                        dataFolder.getFileSystem(new HadoopConfiguration(this.conf.getConf()).getHadoopConf());
                if (fileSystem.exists(dataFolder)) {
                    fileSystem.delete(dataFolder, true);
                }
            } catch (IOException e) {
                log.error("Exception: {}", e.getMessage());
                throw new JobRuntimeException(e);
            }
        }
        super.write(data);
        log.info("Start to rename hdfs files with prefix {}", this.conf.getFileNamePrefix());
        final Path destPath = new Path(this.conf.getPathPrefix());
        try {
            final FileSystem fileSystem =
                    destPath.getFileSystem(new HadoopConfiguration(this.conf.getConf()).getHadoopConf());
            final FileStatus[] status = fileSystem.listStatus(new Path(this.conf.getFullPath()));
            int partitionId = 0;
            for (final FileStatus s : status) {
                if (s.isFile()) {
                    final Path path = s.getPath();
                    final String fileName = path.getName();
                    if (!fileName.equals(SUCCESS) && !fileName.endsWith(CRC)) {
                        final String pathUrl
                                = String.format("%s_%0" + this.digitNum + "d", this.conf.getFullPath(), partitionId);
                        final Path newPath = new Path(pathUrl);
                        fileSystem.rename(path, newPath);
                        partitionId += 1;
                    }
                }
            }
            fileSystem.delete(new Path(this.conf.getFullPath()), true);
            log.info("Finished write files to hdfs path: {}", this.conf.getFullPath());
        } catch (IOException e) {
            log.error("Exception: {}", e);
            throw new JobRuntimeException(e);
        }
    }

}
