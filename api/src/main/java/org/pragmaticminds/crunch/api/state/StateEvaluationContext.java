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
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.events.Event;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * This is a special {@link EvaluationContext} implementation to be used inside a {@link MultiStepEvaluationFunction},
 * to separate it from the outer of {@link MultiStepEvaluationFunction} context.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 07.08.2018
 */
public class StateEvaluationContext<T extends Serializable> extends EvaluationContext<T> {
    private final HashMap<String, T>  events;
    private MRecord values;
    private String alias;

    /**
     * private constructor on base of a {@link MRecord} object
     * @param values
     */
    public StateEvaluationContext(MRecord values, String alias){
        this.values = values;
        this.alias = alias;
        this.events = new HashMap<>();
    }

    // getter
    public Map<String, T> getEvents() {
        return events;
    }

    /**
     * delivers the next {@link MRecord} data to be processed
     *
     * @return the next record to be processed
     */
    @Override
    public MRecord get() {
        return values;
    }

    // setter

    /**
     * sets the current {@link MRecord} data to be processed
     * @param values the record to be processed next
     */
    public void set(MRecord values) {
        this.values = values;
    }

    /**
     * sets the alias for naming resulting {@link Event}s.
     * @param alias name for resulting Events.
     */
    public void setAlias(String alias) {
        this.alias = alias;
    }

    // methods

    /**
     * collects the resulting T event of processing with it's key
     * @param key should be unique, else it overrides the last value
     * @param event to be stored
     */
    public void collect(String key, T event) {
        this.events.put(key, event);
    }

    /**
     * collects the resulting {@link Event} of processing
     * !! do not use the simple collect Method in the {@link MultiStepEvaluationFunction} !!
     * @param event T result of the processing of an {@link EvaluationFunction}
     * @deprecated this collect method is not to be used in the case of this class, instead collect(key, event) is to
     * be used.
     */
    @Override
    @Deprecated
    @SuppressWarnings("squid:S1133") // no reminder to remove the deprecated method needed
    public void collect(T event) {
        this.events.put(alias, event);
    }
}
