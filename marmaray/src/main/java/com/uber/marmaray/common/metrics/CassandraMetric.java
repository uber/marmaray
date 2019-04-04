package com.uber.marmaray.common.metrics;

import com.uber.marmaray.common.configuration.CassandraSinkConfiguration;
import com.uber.marmaray.utilities.StringTypes;
import lombok.NonNull;

import java.util.Map;

public class CassandraMetric {

    public static final String TABLE_NAME_TAG = "tableName";

    public static Map<String, String> createTableNameTags(@NonNull final CassandraSinkConfiguration cassandraConf) {
        return DataFeedMetrics.createAdditionalTags(TABLE_NAME_TAG,
                cassandraConf.getKeyspace() + StringTypes.UNDERSCORE + cassandraConf.getTableName());
    }
}
