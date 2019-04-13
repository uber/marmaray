package com.uber.marmaray.utilities;

import com.google.common.base.Optional;
import lombok.extern.slf4j.Slf4j;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

@Slf4j
public class CassandraSinkUtil {

    public static final TimeZone TIME_ZONE_UTC = TimeZone.getTimeZone("UTC");

    public static Optional<Long> computeTimestamp(final Optional<String> partition) {
        if (partition.isPresent()) {
            try {
                final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                formatter.setTimeZone(TIME_ZONE_UTC);
                final Long epochTime = formatter.parse(partition.get()).getTime() * 1000;
                return Optional.of(epochTime);
            } catch (ParseException e) {
                log.error("Got exception in parse the date to microseconds. {}", e);
            }
        }

        return Optional.absent();
    }
}
