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

package org.pragmaticminds.crunch.examples.windowed;

import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.comparator.Suppliers;
import org.pragmaticminds.crunch.api.trigger.filter.EventFilter;
import org.pragmaticminds.crunch.api.windowed.RecordWindow;
import org.pragmaticminds.crunch.api.windowed.WindowedEvaluationFunction;
import org.pragmaticminds.crunch.api.windowed.Windows;
import org.pragmaticminds.crunch.api.windowed.extractor.DefaultGenericEventGroupAggregationFinalizer;
import org.pragmaticminds.crunch.api.windowed.extractor.GroupByExtractor;
import org.pragmaticminds.crunch.api.windowed.extractor.WindowExtractor;
import org.pragmaticminds.crunch.api.windowed.extractor.aggregate.Aggregations;
import org.pragmaticminds.crunch.events.GenericEvent;
import org.pragmaticminds.crunch.examples.SimpleEvaluationExample;
import org.pragmaticminds.crunch.examples.sources.ValuesGenerator;

import java.util.*;

/**
 * This is an example of the {@link WindowedEvaluationFunction} usage.
 *
 * @author erwin.wagasow
 * created by erwin.wagasow on 21.01.2019
 */
public class WindowedEvaluationFunctionExample extends SimpleEvaluationExample {

    private static final String CHANNEL_DOUBLE = "channel_double";
    private static final String CHANNEL_LONG = "channel_long";
    private static final String CHANNEL_NAME_PREFIX = "channel_";
    private transient List<Map<String, Object>> inData;

    public static void main(String[] args) {
        WindowedEvaluationFunctionExample example = new WindowedEvaluationFunctionExample();
        example.getExecutor().run();
    }

    @Override
    protected EvaluationFunction<GenericEvent> generateEvaluationFunction(Set<String> channels) {
        return WindowedEvaluationFunction.<GenericEvent>builder()
                // determine whether the EvaluationFunctions trigger condition is true
                .recordWindow(generateRecordWindow(channels))
                // extracts and aggregates the triggered on records
                .extractor(generateExtractor())
                // filters the resulting events
                .filter(generateFilter(channels))
                .build();
    }

    /**
     * Generates test data records.
     *
     * @return List of value maps, each one is used to create one Record in the SimpleMRecordSource.
     */
    @Override
    public List<Map<String, Object>> getInData() {
        if(inData == null){
            inData = ValuesGenerator.generateMixedData(100, CHANNEL_NAME_PREFIX);
        }
        return inData;
    }

    /**
     * Generates the definition when a record window is opened and closed again
     *
     * SuppressWarning: squid:S1172 : Not used parameter are to be removed. If any class is inheriting this class, it
     *                                might need to use this parameter.
     */
    @SuppressWarnings("squid:S1172")
    public RecordWindow generateRecordWindow(Set<String> channels) {
        return Windows.bitActive(
                Suppliers.Comparators.equals(
                        true,
                        Suppliers.BooleanOperators.and(
                                Suppliers.Comparators.lowerThanEquals(2L, Suppliers.ChannelExtractors.longChannel(CHANNEL_LONG)),
                                Suppliers.Comparators.greaterThanEquals(4L, Suppliers.ChannelExtractors.longChannel(CHANNEL_LONG))
                        )
                )
        );
    }

    /**
     * Defines the way the values are extracted from the records and how they are to be aggregated.
     *
     * @return new instance of {@link WindowExtractor} of {@link GenericEvent} type.
     */
    public WindowExtractor<GenericEvent> generateExtractor() {
        return GroupByExtractor.<GenericEvent>builder()
                .aggregate(
                        Aggregations.avg(),
                        Suppliers.ChannelExtractors.doubleChannel(CHANNEL_DOUBLE),
                        "avg"
                )
                .aggregate(
                        Aggregations.max(),
                        Suppliers.ChannelExtractors.doubleChannel(CHANNEL_DOUBLE),
                        "max"
                )
                .aggregate(
                        Aggregations.min(),
                        Suppliers.ChannelExtractors.doubleChannel(CHANNEL_DOUBLE),
                        "min"
                )
                .aggregate(
                        Aggregations.sum(),
                        Suppliers.ChannelExtractors.doubleChannel(CHANNEL_DOUBLE),
                        "sum"
                )
                .finalizer(new DefaultGenericEventGroupAggregationFinalizer())
                .build();
    }

    /**
     * Defines filtering rules to only pass relevant resulting GenericEvents.
     *
     * @param channels all channels that are relevant for this processing.
     * @return new instance of the {@link EventFilter} of {@link GenericEvent} type.
     */
    public EventFilter<GenericEvent> generateFilter(Set<String> channels) {
        HashSet<String> channels1 = new HashSet<>(channels);
        return new EventFilter<GenericEvent>() {
            @Override
            public boolean apply(GenericEvent event, MRecord values) {
                // decide which GenericEvents can go through -> all
                return true;
            }

            @Override
            public Collection<String> getChannelIdentifiers() {
                return channels1;
            }
        };
    }
}
