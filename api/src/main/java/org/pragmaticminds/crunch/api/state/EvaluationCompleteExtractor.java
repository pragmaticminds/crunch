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
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.events.GenericEvent;

import java.io.Serializable;
import java.util.Map;

/**
 * This interface if for the extraction of resulting {@link GenericEvent}s after the successful processing of the
 * {@link MultiStepEvaluationFunction} from the resulting Events of all inner {@link EvaluationFunction}s of the
 * {@link MultiStepEvaluationFunction}.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 07.08.2018
 */
@FunctionalInterface
public interface EvaluationCompleteExtractor<T extends Serializable> extends Serializable {

    /**
     * If all states of the {@link MultiStepEvaluationFunction} are done, this method is called.
     * It processes all {@link GenericEvent}s that where passed from inner {@link EvaluationFunction} of the
     * {@link MultiStepEvaluationFunction} and generates outgoing {@link GenericEvent}s, which are than collected by
     * the context
     *
     * @param events incoming values to generate a final resulting T events.
     * @param context has a collect method for the outgoing {@link GenericEvent}s
     */
    void process(Map<String, T> events, EvaluationContext<T> context);
}
