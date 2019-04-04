package com.uber.marmaray.utilities;

import com.uber.marmaray.common.exceptions.JobRuntimeException;

public class TimeUnitUtil {

    private static final Long MICRO_SEC_LOWER_BOUND = (long) Math.pow(10, 15);
    private static final Long MICRO_SEC_HIGHER_BOUND = (long) Math.pow(10, 18);

    /**
     * ConvertToMicroSeconds
     * @param num can only be in these time units [sec, milliseconds, microseconds, nanoseconds]
     * @return microseconds
     */
    public static String convertToMicroSeconds(final Long num) {
        int computeTimes = 4;
        Long val = num;
        while (computeTimes-- > 0) {
            if (val.compareTo(MICRO_SEC_HIGHER_BOUND) >= 0) {
                val /= 1000;
            } else if (val.compareTo(MICRO_SEC_LOWER_BOUND) < 0) {
                val *= 1000;
            } else {
                return String.valueOf(val);
            }
        }

        throw new JobRuntimeException("Input timestamp doesn't have expected time unit. "
                + "We accept only seconds/milliseconds/microseconds/nanoseconds.]");
    }
}
