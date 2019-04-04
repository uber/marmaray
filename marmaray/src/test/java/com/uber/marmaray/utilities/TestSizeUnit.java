package com.uber.marmaray.utilities;

import org.junit.Assert;
import org.junit.Test;

public class TestSizeUnit {

    @Test
    public void testToBits() {
        Assert.assertEquals(17, SizeUnit.BITS.toBits(17));
        Assert.assertEquals(8 * 125, SizeUnit.BYTES.toBits(125));
        Assert.assertEquals(8 * 1024 * 200, SizeUnit.KILOBYTES.toBits(200));
        Assert.assertEquals(3 * 8 * 1024 * 1024, SizeUnit.MEGABYTES.toBits(3));
        Assert.assertEquals(8L * 1024 * 1024 * 1024, SizeUnit.GIGABYTES.toBits(1));
    }

    @Test
    public void testToBytes() {
        Assert.assertEquals(125, SizeUnit.BITS.toBytes(8 * 125));
        Assert.assertEquals(17, SizeUnit.BYTES.toBytes(17));
        Assert.assertEquals(1024 * 200, SizeUnit.KILOBYTES.toBytes(200));
        Assert.assertEquals(3 * 1024 * 1024, SizeUnit.MEGABYTES.toBytes(3));
        Assert.assertEquals(1024 * 1024 * 1024, SizeUnit.GIGABYTES.toBytes(1));
    }

    @Test
    public void testToKilobytes() {
        Assert.assertEquals(125, SizeUnit.BITS.toKilobytes(8 * 125 * 1024));
        Assert.assertEquals(17, SizeUnit.BYTES.toKilobytes(17 * 1024));
        Assert.assertEquals(200, SizeUnit.KILOBYTES.toKilobytes(200));
        Assert.assertEquals(3  * 1024, SizeUnit.MEGABYTES.toKilobytes(3));
        Assert.assertEquals(1024 * 1024, SizeUnit.GIGABYTES.toKilobytes(1));
    }

    @Test
    public void testToMegabytes() {
        Assert.assertEquals(125, SizeUnit.BITS.toMegabytes(8 * 125 * 1024 * 1024));
        Assert.assertEquals(17, SizeUnit.BYTES.toMegabytes(17 * 1024 * 1024));
        Assert.assertEquals(200, SizeUnit.KILOBYTES.toMegabytes(200 * 1024));
        Assert.assertEquals(3, SizeUnit.MEGABYTES.toMegabytes(3));
        Assert.assertEquals(1024, SizeUnit.GIGABYTES.toMegabytes(1));
    }

    @Test
    public void testToGigabytes() {
        Assert.assertEquals(125, SizeUnit.BITS.toGigabytes(8L * 125 * 1024 * 1024 * 1024));
        Assert.assertEquals(17, SizeUnit.BYTES.toGigabytes(17L * 1024 * 1024 * 1024));
        Assert.assertEquals(200, SizeUnit.KILOBYTES.toGigabytes(200 * 1024 * 1024));
        Assert.assertEquals(3, SizeUnit.MEGABYTES.toGigabytes(3 * 1024));
        Assert.assertEquals(1, SizeUnit.GIGABYTES.toGigabytes(1));
        // test underflow rounds to 0
        Assert.assertEquals(0, SizeUnit.BITS.toGigabytes(300));
    }
}
