/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.pragmaticminds.crunch.api.windowed.extractor.aggregate;

import org.junit.Assert;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public class AggregationsTest {
    private static final double DELTA = 0.00001;

    @Test
    public void max() {
        Aggregation<Integer> max = Aggregations.max();
        max.aggregate(1);
        max.aggregate(2);
        max.aggregate(3);
        max.aggregate(4);
        max.aggregate(null);
        double aggregated = max.getAggregated();
        Assert.assertEquals(4, aggregated, DELTA);
        Assert.assertEquals("max", max.getIdentifier());
        max.reset();
    }

    @Test
    public void maxCloned() {
        Aggregation<Integer> max = ClonerUtil.clone(Aggregations.max());
        max.aggregate(1);
        max.aggregate(2);
        max.aggregate(3);
        max.aggregate(4);
        max.aggregate(null);
        double aggregated = max.getAggregated();
        Assert.assertEquals(4, aggregated, DELTA);
        Assert.assertEquals("max", max.getIdentifier());
        max.reset();
    }

    @Test
    public void min() {
        Aggregation<Long> min = Aggregations.min();
        min.aggregate(1L);
        min.aggregate(2L);
        min.aggregate(3L);
        min.aggregate(null);
        min.aggregate(4L);
        double aggregated = min.getAggregated();
        Assert.assertEquals(1L, aggregated, DELTA);
        Assert.assertEquals("min", min.getIdentifier());
        min.reset();
    }

    @Test
    public void minClone() {
        Aggregation<Long> min = ClonerUtil.clone(Aggregations.min());
        min.aggregate(1L);
        min.aggregate(2L);
        min.aggregate(3L);
        min.aggregate(null);
        min.aggregate(4L);
        double aggregated = min.getAggregated();
        Assert.assertEquals(1L, aggregated, DELTA);
        Assert.assertEquals("min", min.getIdentifier());
        min.reset();
    }

    @Test
    public void sum() {
        Aggregation<Float> sum = Aggregations.sum();
        sum.aggregate(1F);
        sum.aggregate(2F);
        sum.aggregate(null);
        sum.aggregate(3F);
        sum.aggregate(4F);
        double aggregated = sum.getAggregated();
        Assert.assertEquals(10F, aggregated, DELTA);
        Assert.assertEquals("sum", sum.getIdentifier());
        sum.reset();
    }

    @Test
    public void sumClone() {
        Aggregation<Float> sum = ClonerUtil.clone(Aggregations.sum());
        sum.aggregate(1F);
        sum.aggregate(2F);
        sum.aggregate(null);
        sum.aggregate(3F);
        sum.aggregate(4F);
        double aggregated = sum.getAggregated();
        Assert.assertEquals(10F, aggregated, DELTA);
        Assert.assertEquals("sum", sum.getIdentifier());
        sum.reset();
    }

    @Test
    public void average() {
        Aggregation<Double> avg = Aggregations.avg();
        avg.aggregate(null);
        avg.aggregate(1D);
        avg.aggregate(2D);
        avg.aggregate(3D);
        avg.aggregate(4D);
        double aggregated = avg.getAggregated();
        Assert.assertEquals(2.5D, aggregated, DELTA);
        Assert.assertEquals("avg", avg.getIdentifier());
        avg.reset();
    }

    @Test
    public void averageClone() {
        Aggregation<Double> avg = ClonerUtil.clone(Aggregations.avg());
        avg.aggregate(null);
        avg.aggregate(1D);
        avg.aggregate(2D);
        avg.aggregate(3D);
        avg.aggregate(4D);
        double aggregated = avg.getAggregated();
        Assert.assertEquals(2.5D, aggregated, DELTA);
        Assert.assertEquals("avg", avg.getIdentifier());
        avg.reset();
    }
}