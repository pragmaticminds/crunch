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

package org.pragmaticminds.crunch.api.pipe;

import org.pragmaticminds.crunch.api.values.TypedValues;

import java.io.Serializable;
import java.util.Set;

/**
 * The general interface of all EvaluationFunctions.
 * It can be implemented as a lambda, cause it has a {@link FunctionalInterface} annotation.
 * It has a eval function for processing an {@link EvaluationContext}, which has an incoming value and handles outgoing
 * results for the {@link EvaluationFunction}.
 *
 * {@link #init()} and {@link #close()} methods added, thus usage of default.
 *
 * @author julian
 * @author Erwin Wagasow
 * created by Erwin Wagasow on 03.08.2018
 */
public interface EvaluationFunction<T extends Serializable> extends Serializable {

    /** Is called before start of evaluation. */
    default void init() { /* Does nothing by default. */ }

    /**
     * evaluates the incoming {@link TypedValues} from the {@link EvaluationContext} and passes the results
     * back to the collect method of the context
     * @param ctx contains incoming data and a collector for the outgoing data
     */
    void eval(EvaluationContext<T> ctx);

    /** Is called after last value is evaluated (if ever). */
    default void close() { /* Does nothing by default. */ }

    /**
     * Returns all channel identifiers which are necessary for the function to do its job.
     * It is not allowed to return null, an empty set can be returned (but why should??).
     *
     * @return a {@link Set} all channel identifiers that are needed by the Evaluation Function.
     */
    Set<String> getChannelIdentifiers();
}
