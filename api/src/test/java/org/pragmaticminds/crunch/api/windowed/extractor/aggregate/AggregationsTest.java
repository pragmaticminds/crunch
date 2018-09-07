package org.pragmaticminds.crunch.api.windowed.extractor.aggregate;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public class AggregationsTest {
    
    @Test
    public void max() {
        Aggregation<Integer, Integer> max = Aggregations.max();
        max.aggregate(1);
        max.aggregate(2);
        max.aggregate(3);
        max.aggregate(4);
        max.aggregate(null);
        int aggregated = max.getAggregated();
        Assert.assertEquals(4, aggregated);
        Assert.assertEquals("max", max.getIdentifier());
        max.reset();
    }
    
    @Test
    public void min() {
        Aggregation<Long, Long> min = Aggregations.min();
        min.aggregate(1L);
        min.aggregate(2L);
        min.aggregate(3L);
        min.aggregate(null);
        min.aggregate(4L);
        long aggregated = min.getAggregated();
        Assert.assertEquals(1L, aggregated);
        Assert.assertEquals("min", min.getIdentifier());
        min.reset();
    }
    
    @Test
    public void sum() {
        Aggregation<Float, Float> sum = Aggregations.sum();
        sum.aggregate(1F);
        sum.aggregate(2F);
        sum.aggregate(null);
        sum.aggregate(3F);
        sum.aggregate(4F);
        float aggregated = sum.getAggregated();
        Assert.assertEquals(10F, aggregated, 0.0001);
        Assert.assertEquals("sum", sum.getIdentifier());
        sum.reset();
    }
    
    @Test
    public void average() {
        Aggregation<Double, Double> avg = Aggregations.avg();
        avg.aggregate(null);
        avg.aggregate(1D);
        avg.aggregate(2D);
        avg.aggregate(3D);
        avg.aggregate(4D);
        double aggregated = avg.getAggregated();
        Assert.assertEquals(2.5D, aggregated, 0.0001);
        Assert.assertEquals("avg", avg.getIdentifier());
        avg.reset();
    }
}