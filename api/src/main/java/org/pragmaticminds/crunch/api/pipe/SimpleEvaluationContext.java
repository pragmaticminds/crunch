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
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of the {@link EvaluationContext} in a simple manner
 *
 * @author Erwin Wagasow
 * created by Erwin Wagasow on 03.08.2018
 */
public class SimpleEvaluationContext<T extends Serializable> extends EvaluationContext<T> {

    private final MRecord values;
    private ArrayList<T> events;

    /**
     * Simple constructor, getting the values to be processed by a {@link EvaluationFunction}
     * @param values to be processed
     */
    public SimpleEvaluationContext(MRecord values) {
        this.values = values;
        this.events = new ArrayList<>();
    }

    /** @inheritDoc */
    @Override
    public MRecord get() {
        return values;
    }

    public List<T> getEvents() {
        return this.events;
    }

    /** @inheritDoc */
    @Override
    public void collect(T event) {
        if(event != null){
            events.add(event);
        }
    }
}
