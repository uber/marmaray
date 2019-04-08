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
package com.uber.marmaray.utilities;

import com.uber.marmaray.common.data.ErrorData;
import com.uber.marmaray.common.data.RawData;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.xml.bind.DatatypeConverter;

@Slf4j
public class KafkaSourceConverterErrorExtractor extends ErrorExtractor {

    @Override
    public String getChangeLogColumns(@NonNull final RawData rawdata) {
        return DEFAULT_CHANGELOG_COLUMNS;
    }

    @Override
    public String getErrorSourceData(@NonNull final ErrorData errorData) {
        try {
            return DatatypeConverter.printHexBinary((byte []) errorData.getRawData().getData());
        } catch (Exception e) {
            log.debug("Not able to retrieve Error source data from ErrorData");
            return DEFAULT_ERROR_SOURCE_DATA;
        }
    }

    @Override
    public String getErrorException(@NonNull final ErrorData errorData) {
        return errorData.getErrMessage();
    }
}
