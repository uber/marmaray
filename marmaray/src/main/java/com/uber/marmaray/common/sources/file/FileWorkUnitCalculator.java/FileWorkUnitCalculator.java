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

package com.uber.marmaray.common.sources.file;

import com.uber.marmaray.common.configuration.FileSourceConfiguration;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.metadata.IMetadataManager;
import com.uber.marmaray.common.metadata.StringValue;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.IChargebackCalculator;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.common.sources.IWorkUnitCalculator;
import com.uber.marmaray.common.sources.file.FileWorkUnitCalculator.FileWorkUnitCalculatorResult;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
public class FileWorkUnitCalculator implements IWorkUnitCalculator<
    LocatedFileStatus, FileRunState, FileWorkUnitCalculatorResult, StringValue> {

    @Getter
    private final FileSourceConfiguration conf;

    @Override
    public void setDataFeedMetrics(@NonNull final DataFeedMetrics dataFeedMetrics) {
        // ignored
    }

    @Override
    public void setJobMetrics(@NonNull final JobMetrics jobMetrics) {
        // ignored
    }

    @Override
    public void initPreviousRunState(@NonNull final IMetadataManager<StringValue> metadataManager) {
        // ignoring run state; just ingesting all files in a directory
    }

    @Override
    public void saveNextRunState(@NonNull final IMetadataManager<StringValue> metadataManager,
                                 @NonNull final FileRunState nextRunState) {
        // still ignoring run state, just loading all files

    }

    @Override
    public FileWorkUnitCalculatorResult computeWorkUnits() {
        try {
            final List<LocatedFileStatus> results = new ArrayList<>();
            final FileSystem fs = this.conf.getFileSystem();
            final RemoteIterator<LocatedFileStatus> fileStatuses = fs.listFiles(this.conf.getDirectory(), false);
            final String correctSuffix = "." + this.conf.getType();
            while (fileStatuses.hasNext()) {
                final LocatedFileStatus fileStatus = fileStatuses.next();
                if (fileStatus.getPath().getName().endsWith(correctSuffix)) {
                    results.add(fileStatus);
                }
            }
            return new FileWorkUnitCalculatorResult(results);
        } catch (IOException e) {
            throw new JobRuntimeException(
                String.format("Error getting work units for directory %s", this.conf.getDirectory()), e);
        }
    }

    @Override
    public void setChargebackCalculator(@NonNull final IChargebackCalculator calculator) {
        // not charging back for now
    }

    public final class FileWorkUnitCalculatorResult implements IWorkUnitCalculator.IWorkUnitCalculatorResult<
        LocatedFileStatus, FileRunState> {

        private final List<LocatedFileStatus> workUnits;

        public FileWorkUnitCalculatorResult(@NonNull final List<LocatedFileStatus> workUnits) {
            this.workUnits = workUnits;
        }

        @Override
        public List<LocatedFileStatus> getWorkUnits() {
            return this.workUnits;
        }

        @Override
        public boolean hasWorkUnits() {
            return this.workUnits.size() > 0;
        }

        @Override
        public FileRunState getNextRunState() {
            return new FileRunState();
        }
    }
}
