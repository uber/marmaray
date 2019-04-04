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
package com.uber.marmaray.common.metrics;

        import com.uber.marmaray.common.exceptions.JobRuntimeException;

public final class ErrorCauseTagNames {

    public static final String FILE_PATH = "file_path";
    public static final String MEMORY = "memory";
    public static final String PERMISSON = "permission";
    public static final String SCHEMA = "schema";
    public static final String NO_DATA = "no_data";

    public static final String ERROR = "error";
    public static final String CONFIG_ERROR = "config_error";

    // FS
    public static final String FS_UTIL = "fs_util";
    public static final String FS_UTIL_DELETE = "fs_util_delete";

    // SOURCE
    public static final String NO_FILE = "file_does_not_exist";
    public static final String EMPTY_PATH = "no_file_in_data_path";

    // SINK
    public static final String COMPRESSION = "compression";
    public static final String OUTPUT_FILE_FORMAT = "output_file_format";
    public static final String CALCULATE_SAMPLE_SIZE = "calculate_sample_size";
    public static final String WRITE_FILE = "write_file";
    public static final String WRITE_TO_SINK = "write_to_sink";
    public static final String WRITE_TO_SINK_CSV = "write_to_sink_csv";
    public static final String WRITE_TO_SINK_SEQ = "write_to_sink_sequence";
    public static final String WRITE_TO_CASSANDRA = "write_to_sink_cassandra";
    public static final String CONNECT_TO_SINK = "connect_to_sink";

    //AWS
    public static final String AWS = "aws";
    public static final String UPLOAD = "upload_to_s3";

    // metadata
    public static final String SAVE_METADATA = "saving_metadata";
    public static final String LOAD_METADATA = "loading_metadata";

    // cassandra
    public static final String CASSANDRA = "cassandra";
    public static final String CASSANDRA_QUERY = "cassandra_query";
    public static final String EXEC_CASSANDRA_CMD = "exec_cassandra_cmd";

    //converter
    public static final String DATA_CONVERSION = "data_conversion";
    public static final String NOT_SUPPORTED_FIELD_TYPE = "not_supported_field_type";
    public static final String MISSING_FIELD = "missing_field";
    public static final String MISSING_DATA = "missing_data";
    public static final String EMPTY_FIELD = "empty_field";

    // schema manager
    public static final String CLUSTER_KEY = "cluster_key_missing/not_found";
    public static final String PARTITION_KEY = "partition_key_missing/not_found";
    public static final String KEYSPACE = "keyspace_is_missing";
    public static final String TABLE = "table_name_is_missing";
    public static final String SCHEMA_FIELD = "schema_field_is_missing";
    public static final String PARTITION_CLUSTERING_KEYS_OVERLAP = "partition_clustering_keys_overlap";

    // job manager
    public static final String TIME_OUT = "time_out";

    private ErrorCauseTagNames() {
        throw new JobRuntimeException("Class should never be instantiated");
    }
}
