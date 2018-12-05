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
 * This abstract class accumulates all given values and returns am aggregated value, when called.
 * It also has an identifier, which can be read or set.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public interface Aggregation<T extends Serializable> extends Serializable {
    /**
     * getter for identifier
     * @return the identifier of this implementation
     */
    String getIdentifier();

    /**
     * Takes the given value and stores it until the getAggregated method is called.
     * @param value to be aggregated.
     */
    void aggregate(T value);

    /** @return the aggregated value of all values that have been aggregated by the aggregate method. */
    Double getAggregated();

    /** cleans up the accumulated values */
    void reset();
}
