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

import org.junit.Test;

import java.time.Instant;
import java.util.Date;

import static org.junit.Assert.assertEquals;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public class AggregationUtilsTest {

    private double delta = 0.00001;

    @Test
    public void compare() {
        Instant now = Instant.now();

        assertEquals(0, AggregationUtils.compare(1, 1));
        assertEquals(0, AggregationUtils.compare(1L, 1L));
        assertEquals(0, AggregationUtils.compare(1F, 1F));
        assertEquals(0, AggregationUtils.compare(1D, 1D));
        assertEquals(0, AggregationUtils.compare("1", "1"));
        assertEquals(0, AggregationUtils.compare(Date.from(now), Date.from(now)));
        assertEquals(0, AggregationUtils.compare(now, now));
        assertEquals(0, AggregationUtils.compare(now, now.toString()));
        assertEquals(0, AggregationUtils.compare(1, "1"));
        assertEquals(0, AggregationUtils.compare(1D, 1L));
    }

    @Test
    public void add() {
        assertEquals(2, AggregationUtils.add(1, 1), delta);
        assertEquals(2, AggregationUtils.add(1L, 1L), delta);
        assertEquals(2, AggregationUtils.add(1F, 1F), delta);
        assertEquals(2, AggregationUtils.add(1D, 1D), delta);
        assertEquals(2, AggregationUtils.add(1D, 1L), delta);
    }

    @Test
    public void subtract() {
        assertEquals(1, AggregationUtils.subtract(2, 1), delta);
        assertEquals(1, AggregationUtils.subtract(2L, 1L), delta);
        assertEquals(1, AggregationUtils.subtract(2F, 1F), delta);
        assertEquals(1, AggregationUtils.subtract(2D, 1D), delta);
        assertEquals(1, AggregationUtils.subtract(2D, 1L), delta);
    }

    @Test
    public void multiply() {
        assertEquals(100, AggregationUtils.multiply(10, 10), delta);
        assertEquals(100, AggregationUtils.multiply(10L, 10F), delta);
        assertEquals(100, AggregationUtils.multiply(10F, 10D), delta);
        assertEquals(100, AggregationUtils.multiply(10D, 10L), delta);
    }

    @Test
    public void divide() {
        assertEquals(1, AggregationUtils.divide(10, 10), delta);
        assertEquals(1, AggregationUtils.divide(10L, 10F), delta);
        assertEquals(1, AggregationUtils.divide(10F, 10D), delta);
        assertEquals(1, AggregationUtils.divide(10D, 10L), delta);
    }
}