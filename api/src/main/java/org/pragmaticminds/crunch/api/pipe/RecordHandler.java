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

import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.TypedValues;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Similar to an {@link EvaluationFunction} this class can be added to a {@link EvaluationPipeline}.
 * It can be implemented as a lambda, cause it has a {@link FunctionalInterface} annotation.
 *
 * @author Erwin Wagasow
 * created by Erwin Wagasow on 26.09.2018
 */
public interface RecordHandler extends Serializable {

    /**
     * Is called before start of evaluation.
     */
    void init();

    /**
     * evaluates the incoming {@link TypedValues} and stores it in the inner sink.
     * @param record contains incoming data
     */
    void apply(MRecord record);

    /**
     * Is called after last value is evaluated (if ever).
     */
    void close();

    /**
     * Collects all channel identifiers that are used in the record handler
     * @return a {@link List} or {@link Collection} of all channel identifiers
     */
    Set<String> getChannelIdentifiers();
}
