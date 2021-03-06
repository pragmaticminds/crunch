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

/**
 * Aggregates a minimum value
 * @param <T> type of value
 *
 * @author Erwin Wagasow
 * craeted by Erwin Wagasow on 23.08.2018
 */
class Min<T extends Number & Comparable> implements Aggregation<T> {
    private Double minValue;

    /**
     * @return default identifier
     */
    @Override
    public String getIdentifier() {
        return "min";
    }

    /**
     * Compares the current minimum value the given value
     * @param value to be aggregated.
     */
    @Override
    public void aggregate(T value) {
        if(value != null && (minValue == null || AggregationUtils.compare(value, minValue) < 0)){
            minValue = value.doubleValue();
        }
    }

    /**
     * @return the minimum value so far
     */
    @Override
    public Double getAggregated() {
        return minValue;
    }

    /**
     * sets the minimum value so far to null
     */
    @Override
    public void reset() {
        minValue = null;
    }
}
