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

package org.pragmaticminds.crunch.api.trigger.handler;

import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.trigger.extractor.MapExtractor;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.GenericEvent;
import org.pragmaticminds.crunch.events.GenericEventBuilder;

import java.util.Collection;
import java.util.Map;

/**
 * This class implements the {@link #createEvent(String, EvaluationContext, Map)} method, which is abstract in
 * the {@link ExtractorTriggerHandler} class.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 01.10.2018
 */
public class GenericExtractorTriggerHandler extends ExtractorTriggerHandler<GenericEvent> {
    /** @inheritDoc */
    public GenericExtractorTriggerHandler(String eventName, MapExtractor... extractors) {
        super(eventName, extractors);
    }

    /** @inheritDoc */
    public GenericExtractorTriggerHandler(String eventName, Collection<MapExtractor> extractors) {
        super(eventName, extractors);
    }

    /**
     * This method creates a resulting {@link GenericEvent}.
     *
     * @param eventName name of the new GenericEvent.
     * @param context current
     * @param parameters Map of {@link String} to {@link Value}
     * @return the created GenericEvent
     */
    @Override
    protected GenericEvent createEvent(
            String eventName, EvaluationContext<GenericEvent> context, Map<String, Value> parameters
    ) {
        return GenericEventBuilder.anEvent()
                .withEvent(eventName)
                .withSource(context.get().getSource())
                .withTimestamp(context.get().getTimestamp())
                // set the resulting String Value Map from the extractors
                .withParameters(parameters)
                .build();
    }
}
