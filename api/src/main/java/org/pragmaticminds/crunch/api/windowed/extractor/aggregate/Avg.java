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
 * Aggregates all values to calculate am avg value as {@link Double}
 * @param <T> type of the incoming values
 *
 * @author Erwin Wagasow
 * craeted by Erwin Wagasow on 23.08.2018
 */
class Avg<T extends Number> implements Aggregation<T>{
    private Double sum;
    private int count = 0;

    /**
     * @return the default identifier of this class
     */
    @Override
    public String getIdentifier() {
        return "avg";
    }

    /**
     * collects one value, all values are passed to this method to be aggregated
     * @param value to be aggregated.
     */
    @Override
    public void aggregate(T value) {
        // ignore null values
        if(value == null){
            return;
        }
        count++;
        if(sum == null){
            sum = value.doubleValue();
        }else{
            sum = AggregationUtils.add(sum, value);
        }
    }

    /**
     * @return the Aggregation result
     */
    @Override
    public Double getAggregated() {
        return AggregationUtils.divide(sum, count);
    }

    /**
     * resets this structure
     */
    @Override
    public void reset() {
        sum = null;
        count = 0;
    }
}
