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
 * Aggregates a maximal value.
 * Should not be directly visible.
 * @param <T> should be something {@link Comparable}
 *
 * @author Erwin Wagasow
 * craeted by Erwin Wagasow on 23.08.2018
 */
class Max<T extends Number & Comparable> implements Aggregation<T> {
    private Double maxValue;

    /**
     * @return default identifier
     */
    @Override
    public String getIdentifier() {
        return "max";
    }

    /**
     * compares if value is bigger than the last one
     * @param value to be aggregated.
     */
    @Override
    public void aggregate(T value) {
        if(value == null){
            return;
        }
        double innerValue = value.doubleValue();
        if(maxValue == null || AggregationUtils.compare(innerValue, maxValue) > 0){
            maxValue = value.doubleValue();
        }
    }

    /**
     * @return the maximum value so far
     */
    @Override
    public Double getAggregated() {
        return maxValue;
    }

    /**
     * unsets the maximum value so far
     */
    @Override
    public void reset() {
        maxValue = null;
    }
}
