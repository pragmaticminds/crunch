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

package org.pragmaticminds.crunch.api.windowed.extractor;

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.events.GenericEvent;

import java.io.Serializable;
import java.util.Map;

/**
 * This represents the last step of processing in a {@link GroupByExtractor}. This class gets a map of named resulting
 * values. With this {@link Map} this class creates resulting {@link GenericEvent}s which than go out for processing of the
 * next steps.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
@FunctionalInterface
public interface GroupAggregationFinalizer<T extends Serializable> extends Serializable {

    /**
     * Packs the aggregated values into resulting {@link GenericEvent}s.
     *
     * @param aggregatedValues is a map of all aggregated values, that can be further processed and be added as
     *                         parameters into the resulting {@link GenericEvent}s.
     * @param context current from the evaluation call. Takes the resulting {@link GenericEvent}s, with the aggregated values
     *                as parameters.
     */
    void onFinalize(Map<String, Object> aggregatedValues, EvaluationContext<T> context);
}
