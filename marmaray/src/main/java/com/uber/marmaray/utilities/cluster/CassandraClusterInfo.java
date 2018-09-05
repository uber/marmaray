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
package com.uber.marmaray.utilities.cluster;

import com.google.common.base.Optional;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * POJO object that contains all the information
 * related to the cassandra cluster
 */
public class CassandraClusterInfo {

    @Getter
    private final Optional<String> sslStoragePort;

    @Getter
    private final Optional<String> rpcPort;

    @Getter
    private final Optional<String> storagePort;

    @Getter
    @Setter
    private Optional<String> nativeApplicationPort;

    @Getter
    @Setter
    private Optional<String> listOfNodes;

    public CassandraClusterInfo(@NonNull final Optional<String> sslStoragePart,
                                @NonNull final Optional<String> rpcPort,
                                @NonNull final Optional<String> storagePort) {
        this.sslStoragePort = sslStoragePart;
        this.rpcPort = rpcPort;
        this.storagePort = storagePort;
        this.nativeApplicationPort = Optional.absent();
        this.listOfNodes = Optional.absent();
    }
}
