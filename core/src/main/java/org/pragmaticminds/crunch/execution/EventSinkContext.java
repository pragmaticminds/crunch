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

package org.pragmaticminds.crunch.execution;

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.events.Event;

import java.io.Serializable;

/**
 * This is an {@link EvaluationContext} which forwards all the {@link Event}s it receives to a
 * {@link EventSink} callback.
 * <p>
 * It does this blocking, so {@link EventSink} implementations should be careful not to block (too much) on this.
 *
 * @author julian
 * Created by julian on 15.08.18
 */
public class EventSinkContext<T extends Serializable> extends EvaluationContext<T> {

    private final EventSink<T> sink;
    private MRecord current;

    public EventSinkContext(EventSink<T> sink) {
        this.sink = sink;
    }

    /**
     * Sets the current record.
     * Has to be done before {@link #get()} is invoked.
     *
     * @param current Current Value
     */
    public void setCurrent(MRecord current) {
        this.current = current;
    }

    @Override
    public MRecord get() {
        return current;
    }

    @Override
    public void collect(T event) {
        sink.handle(event);
    }
}
