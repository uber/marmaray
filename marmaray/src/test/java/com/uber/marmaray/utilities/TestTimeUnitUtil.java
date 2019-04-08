package com.uber.marmaray.utilities;

import org.junit.Assert;
import org.junit.Test;

public class TestTimeUnitUtil {

    @Test
    public void testConvertToMicroSeconds() {
        // millsec -> microsec;
        Assert.assertEquals("1551742037000000", TimeUnitUtil.convertToMicroSeconds(1551742037L));

        // sec -> microsec;
        Assert.assertEquals("1551742000000000", TimeUnitUtil.convertToMicroSeconds(1551742L));

        // microsec -> microsec;
        Assert.assertEquals("1551742037895764", TimeUnitUtil.convertToMicroSeconds(1551742037895764L));

        // nanosec -> microsec;
        Assert.assertEquals("1551742037895764", TimeUnitUtil.convertToMicroSeconds(1551742037895764000L));

        // microsec_higer_bound -> microsec;
        Assert.assertEquals("1000000000000000", TimeUnitUtil.convertToMicroSeconds((long) Math.pow(10, 18)));

        // microsec_lower_bound -> microsec;
        Assert.assertEquals("1000000000000000", TimeUnitUtil.convertToMicroSeconds((long) Math.pow(10, 15)));
    }
}
