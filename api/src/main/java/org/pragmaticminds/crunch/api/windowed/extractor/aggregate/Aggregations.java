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

import java.io.Serializable;

/**
 * This is a collection of {@link Aggregation} implementations for usual use cases.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public class Aggregations implements Serializable {
    private Aggregations() { /* hide the constructor */ }

    /**
     * Creates an implementation of {@link Aggregation} that searches for the biggest value in the aggregated values.
     * @param <T> type of the values
     * @return the biggest value
     */
    public static <T extends Number & Comparable> Aggregation<T> max(){
        return new Max<>();
    }

    /**
     * Creates an implementation of {@link Aggregation} that searches for the smallest value in the aggregated values.
     * @param <T> type of the values
     * @return the smallest value
     */
    public static <T extends Number & Comparable> Aggregation<T> min(){
        return new Min<>();
    }

    /**
     * Creates an implementation of {@link Aggregation} that sums up all aggregated values.
     * @param <T> type of the values
     * @return the sum of all aggregated values
     */
    public static <T extends Number> Aggregation<T> sum(){
        return new Sum<>();
    }

    /**
     * Creates an implementation of {@link Aggregation} that calculates the avg value of accumulated values
     * @param <T> type of the values
     * @return the calculated avg value from aggregated values
     */
    public static <T extends Number> Aggregation<T> avg(){
        return new Avg<>();
    }
}
