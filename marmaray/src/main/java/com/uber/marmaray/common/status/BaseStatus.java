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

package com.uber.marmaray.common.status;

import lombok.Getter;
import lombok.NonNull;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Base implementation of the {@link IStatus} interface. Defaults to success, but status can be downgraded.
 */
public class BaseStatus implements IStatus {

    @Getter
    private Status status = Status.SUCCESS;
    @Getter
    private final List<Exception> exceptions = new LinkedList<>();

    public void setStatus(@NonNull final Status inputStatus) {
        if (inputStatus.compareTo(this.status) > 0) {
            this.status = inputStatus;
        }
    }

    public void addException(@NonNull final Exception t) {
        this.exceptions.add(t);
    }

    public void addExceptions(@NonNull final Collection<Exception> throwableCollection) {
        throwableCollection.forEach(this::addException);
    }

    public void mergeStatus(@NonNull final IStatus inputStatus) {
        setStatus(inputStatus.getStatus());
        this.addExceptions(inputStatus.getExceptions());
    }
}
