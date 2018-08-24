package org.pragmaticminds.crunch.api.windowed.extractor.aggregate;

import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.Date;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public class AggregationUtilsTest {
    
    private double delta;
    
    @Test
    public void comapre() {
        // integer
        int compare = AggregationUtils.compare(1, 1);
        Assert.assertEquals(0, compare);
        // long
        compare = AggregationUtils.compare(1L, 1L);
        Assert.assertEquals(0, compare);
        // float
        compare = AggregationUtils.compare(1F, 1F);
        Assert.assertEquals(0, compare);
        // double
        compare = AggregationUtils.compare(1D, 1D);
        Assert.assertEquals(0, compare);
        // string
        compare = AggregationUtils.compare("1", "1");
        Assert.assertEquals(0, compare);
        // Date
        Instant now = Instant.now();
        compare = AggregationUtils.compare(Date.from(now), Date.from(now));
        Assert.assertEquals(0, compare);
        // Instant
        compare = AggregationUtils.compare(now, now);
        Assert.assertEquals(0, compare);
    }
    
    @Test
    public void sum() {
        // integer
        int sumInteger = AggregationUtils.sum(1, 1);
        Assert.assertEquals(2, sumInteger);
        // long
        long sumLong = AggregationUtils.sum(1L, 1L);
        Assert.assertEquals(2L, sumLong);
        // float
        float sumFloat = AggregationUtils.sum(1F, 1F);
        Assert.assertEquals(2F, sumFloat, 0.0001);
        // double
        double sumDouble = AggregationUtils.sum(1D, 1D);
        Assert.assertEquals(2D, sumDouble, 0.0001);
    }
    
    @Test
    public void divide() {
        // integer
        double divideInteger = AggregationUtils.divide(1, 1);
        delta = 0.0001;
        Assert.assertEquals(1.0, divideInteger, delta);
        // long
        double divideLong = AggregationUtils.divide(1L, 1);
        Assert.assertEquals(1L, divideLong, delta);
        // float
        double divideFloat = AggregationUtils.divide(1F, 1);
        Assert.assertEquals(1F, divideFloat, delta);
        // double
        double divideDouble = AggregationUtils.divide(1D, 1);
        Assert.assertEquals(1D, divideDouble, delta);
    }
}