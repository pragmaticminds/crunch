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

import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * Interface for the creation or reuse of EvaluationFunctions in a {@link MultiStepEvaluationFunction}
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 07.08.2018
 */
public interface EvaluationFunctionStateFactory<T extends Serializable> extends Serializable {

    /**
     * Creates or resets an instance of a {@link EvaluationFunction} to be used in the
     * {@link MultiStepEvaluationFunction}.
     *
     * @return an instance of {@link EvaluationFunction}
     */
    EvaluationFunction<T> create();

    /**
     * Collects all channel identifiers that are used in the inner {@link EvaluationFunction}.
     *
     * @return a {@link List} or {@link Collection} that contains all channel identifiers that are used.
     */
    Collection<String> getChannelIdentifiers();
}
