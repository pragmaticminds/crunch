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

import java.io.Serializable;

/**
 * The context for evaluation in a {@link EvaluationFunction} containing ways to process incoming and
 * outgoing data.
 *
 * @author Erwin Wagasow
 * created by Erwin Wagasow on 03.08.2018
 */
@SuppressWarnings("squid:S1610") // not converting into an interface
public abstract class EvaluationContext<T extends Serializable> implements Serializable {
    /**
     * delivers the next {@link MRecord} data to be processed
     * @return the next record to be processed
     */
    public abstract MRecord get();

    /**
     * collects the resulting Event object of processing
     * @param event result of the processing of an {@link EvaluationFunction}
     */
    public abstract void collect(T event);
}
