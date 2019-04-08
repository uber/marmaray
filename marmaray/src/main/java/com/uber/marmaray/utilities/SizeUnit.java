package com.uber.marmaray.utilities;

/**
 * Conversion class for size units (bits and bytes) using 1024 as a factor between the levels.
 * Similar to {@link java.util.concurrent.TimeUnit}.
 *
 * Note that conversions may overflow Long when going from extremely large coarse units to finer ones.
 */
public enum SizeUnit {

    BITS(Constants.BIT_FACTOR),
    BYTES(Constants.BYTE_FACTOR),
    KILOBYTES(Constants.KILOBYTE_FACTOR),
    MEGABYTES(Constants.MEGABYTE_FACTOR),
    GIGABYTES(Constants.GIGABYTE_FACTOR);

    private final long factor;

    SizeUnit(final long factor) {
        this.factor = factor;
    }

    public long toBits(final long input) {
        return this.factor * input;
    }

    public long toBytes(final long input) {
        return this.factor * input / Constants.BYTE_FACTOR;
    }

    public long toKilobytes(final long input) {
        return this.factor * input / Constants.KILOBYTE_FACTOR;
    }

    public long toMegabytes(final long input) {
        return this.factor * input / Constants.MEGABYTE_FACTOR;
    }

    public long toGigabytes(final long input) {
        return this.factor * input / Constants.GIGABYTE_FACTOR;
    }

    private static final class Constants {
        public static final int UNIT_SEPARATOR = 1024;
        public static final long BIT_FACTOR = 1;
        public static final long BYTE_FACTOR = 8;
        public static final long KILOBYTE_FACTOR = BYTE_FACTOR * UNIT_SEPARATOR;
        public static final long MEGABYTE_FACTOR = KILOBYTE_FACTOR * UNIT_SEPARATOR;
        public static final long GIGABYTE_FACTOR = MEGABYTE_FACTOR * UNIT_SEPARATOR;
    }
}
