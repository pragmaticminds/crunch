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

package org.pragmaticminds.crunch.api.state;

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.events.GenericEvent;

import java.io.Serializable;
import java.util.Map;

/**
 * This extractor class processes error situations that happen while processing in the {@link MultiStepEvaluationFunction}
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 07.08.2018
 */
@FunctionalInterface
public interface ErrorExtractor<T extends Serializable> extends Serializable {

    /**
     * If any states of the {@link MultiStepEvaluationFunction} produce an Exception or a timeout exception is raised,
     * this method is called.
     * This processes the so far generated result events and the occurred exception to generate final outgoing
     * {@link GenericEvent}s
     *
     * @param events incoming values to generate a final resulting events
     * @param ex {@link Exception} of the error situation
     * @param context has a collect method for the outgoing T events
     */
    void process(Map<String, T> events, Exception ex, EvaluationContext<T> context);
}
