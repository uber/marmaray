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

package com.uber.marmaray.common.converters.converterresult;

import com.google.common.base.Optional;
import com.uber.marmaray.common.data.ErrorData;
import com.uber.marmaray.common.data.RawData;
import com.uber.marmaray.common.data.ValidData;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.Serializable;

/**
 * {@link ConverterResult} associates data of an input type with the converted data with an output type
 *
 * User has option to implement a conversion data of type OD to type ID in a one-way transformation as needed
 *
 * This class is used to maintain parity between the ID data that was converted to OD data as needed for
 * any reporting and error handling purposes.
 *
 * If ID is empty we assume that the OD data was valid and schema-conforming and the original input data is
 * no longer needed.  If ID is non-empty there was an issue with the converted data and it was non-schema conforming.
 *
 * @param <ID> Data Type to Convert to
 * @param <OD> Original Data Type to Convert from
 */
@Slf4j
public class ConverterResult<ID, OD> implements Serializable {

    @NonNull
    @Getter
    protected Optional<ValidData<OD>> successData;

    @NonNull
    @Getter
    protected Optional<ErrorData<ID>> errorData;

    /**
     * Constructor for case that OD is schema conforming
     * @param successData
     */
    public ConverterResult(@NonNull final OD successData) {
        this.successData = Optional.of(new ValidData<>(successData));
        this.errorData = Optional.absent();
    }

    public ConverterResult(@NonNull final ID errorData, @NotEmpty final String errorMessage) {
        this.successData = Optional.absent();
        this.errorData = Optional.of(new ErrorData<>(errorMessage, new RawData<>(errorData)));

    }

    public ConverterResult(@NonNull final ID errorData,
                           @NotEmpty final String errorMessage,
                           @NonNull final OD successData) {
        this.successData = Optional.of(new ValidData<>(successData));
        this.errorData = Optional.of(new ErrorData<>(errorMessage, new RawData<>(errorData)));
    }
}
