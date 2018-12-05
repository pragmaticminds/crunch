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

package org.pragmaticminds.crunch.api;

import org.pragmaticminds.crunch.api.events.GenericEventHandler;
import org.pragmaticminds.crunch.api.function.def.FunctionDef;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.io.Serializable;
import java.util.Map;

/**
 * The default approach to implement an evaluation function
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 19.10.2017
 *
 * @deprecated Part of the old API
 */
@Deprecated
public abstract class EvalFunction<T> implements Serializable {

    private transient GenericEventHandler eventHandler;

    public GenericEventHandler getEventHandler() {
        return eventHandler;
    }

    public void setEventHandler(GenericEventHandler eventHandler) {
        this.eventHandler = eventHandler;
    }

    /**
     * @return a Structure that defines all properties of the current {@link EvalFunction} implementation
     */
    public abstract FunctionDef getFunctionDef();

    /**
     * is started before the processing of the records begins
     *
     * @param literals     the constant in values
     * @param eventHandler the interface to fire results into the system
     */
    public abstract void setup(Map<String, Value> literals, GenericEventHandler eventHandler);

    /**
     * processes single record values
     *
     * @param timestamp the corresponding time stamp for the record
     * @param channels  contains the values of a record
     * @return output value of this {@link EvalFunction}
     */
    public abstract T eval(long timestamp, Map<String, Value> channels);

    /**
     * is called after the processing of all records for cleaning up and firing the last results
     */
    public abstract void finish();

}
