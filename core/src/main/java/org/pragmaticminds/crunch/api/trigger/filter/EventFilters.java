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

package org.pragmaticminds.crunch.api.trigger.filter;

import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;
import org.pragmaticminds.crunch.events.Event;

import java.io.Serializable;
import java.util.Collection;

/**
 * This is a collection of filters for usual use cases
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 14.08.2018
 */
public class EventFilters {
    /** hidden constructor */
    private EventFilters(){
        throw new UnsupportedOperationException("this constructor should never be calles!");
    }

    /**
     * This EventFilter checks if a particular value in the {@link MRecord} has changed since the last processing
     *
     * @param supplier extracts the value of interest out of the {@link MRecord}
     * @return true if the value of interest has been changed since the last processing
     *
     * SuppressWarnings : squid:S00119 : Usage of named templates instead of one character
     */
    @SuppressWarnings("squid:S00119")
    public static <EVENT extends Serializable, T extends Serializable> EventFilter<EVENT> onValueChanged(Supplier<T> supplier) {
        return new EventFilter<EVENT>() {
            private T lastValue;

            /**
             * Applies the filtering checking on the current {@link Event} and {@link MRecord}
             *
             * @param event the extracted from the processing
             * @param values the processed values
             * @return true if this {@link Event} is to be filtered out, otherwise false
             */
            @Override
            public boolean apply(EVENT event, MRecord values) {
                boolean keep = false; // filter by default
                T value = supplier.extract(values);
                if(value == null){
                    return false;
                }
                if(lastValue != null){
                    keep = !value.equals(lastValue);
                }
                lastValue = value;
                return keep;
            }

            @Override
            public Collection<String> getChannelIdentifiers() {
                return supplier.getChannelIdentifiers();
            }
        };
    }
}
