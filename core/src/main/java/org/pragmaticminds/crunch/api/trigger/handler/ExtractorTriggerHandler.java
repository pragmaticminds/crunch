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
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.TriggerEvaluationFunction;
import org.pragmaticminds.crunch.api.trigger.extractor.MapExtractor;
import org.pragmaticminds.crunch.api.trigger.strategy.TriggerStrategy;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.Event;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handles the extraction of the resulting Event, when a {@link TriggerStrategy} in a {@link TriggerEvaluationFunction}
 * was triggered.
 * This implementation of the TriggerHandler holds a {@link List} of {@link MapExtractor}s, which are extracting
 * {@link Value}s of interest from an {@link MRecord}.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 19.09.2018
 */
public abstract class ExtractorTriggerHandler<T extends Serializable> implements TriggerHandler<T> {
    private String eventName;
    private ArrayList<MapExtractor> extractors = null;

    /**
     * Constructor with array of {@link MapExtractor}s.
     *
     * @param eventName Name of the resulting {@link Event}s.
     * @param extractors Array of extractors, which delivers the parameters for the resulting {@link Event}s.
     */
    public ExtractorTriggerHandler(String eventName, MapExtractor... extractors) {
        this.eventName = eventName;
        if(extractors != null && extractors.length != 0){
            this.extractors = new ArrayList<>(Arrays.asList(extractors));
        }
    }

    /**
     * Constructor with {@link Collection}/{@link List} of {@link MapExtractor}s.
     *
     * @param eventName Name of the resulting {@link Event}s.
     * @param extractors {@link Collection}/{@link List} of extractors, which delivers the parameters for the
     *                   resulting {@link Event}s.
     */
    public ExtractorTriggerHandler(String eventName, Collection<MapExtractor> extractors) {
        this.eventName = eventName;
        if(extractors == null){
            return;
        }
        this.extractors = new ArrayList<>(extractors);
    }

    /**
     * When a {@link TriggerEvaluationFunction} is triggered, it calls this method to generate a proper result.
     *
     * @param context of the current processing. It holds the current MRecord and it takes the resulting {@link Event}
     *                objects.
     */
    @Override
    public void handle(EvaluationContext<T> context) {
        // when nothing is set -> no results
        if(extractors == null || extractors.isEmpty()){
            return;
        }

        // merge the results of all extractors into one map
        Map<String, Value> results = extractors.stream()
                // let all extractors extract their result data
                .flatMap(
                        extractor ->
                                extractor
                                        .extract(context)
                                        .entrySet()
                                        .stream()
                )
                // combine all maps into one
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue
                        )
                );

        // collect the resulting Event with the context
        context.collect(
                createEvent(eventName, context, results)
        );
    }

    protected abstract T createEvent(String eventName, EvaluationContext<T> context, Map<String, Value> results);

  @Override public Set<String> getChannelIdentifiers() {
    return this.extractors.stream()
        .flatMap(ex -> ex.getChannelIdentifiers().stream())
        .collect(Collectors.toSet());
  }
}
