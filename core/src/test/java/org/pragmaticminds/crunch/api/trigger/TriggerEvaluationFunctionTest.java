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

package org.pragmaticminds.crunch.api.trigger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.SimpleEvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.extractor.Extractors;
import org.pragmaticminds.crunch.api.trigger.extractor.MapExtractor;
import org.pragmaticminds.crunch.api.trigger.filter.EventFilter;
import org.pragmaticminds.crunch.api.trigger.handler.GenericExtractorTriggerHandler;
import org.pragmaticminds.crunch.api.trigger.handler.TriggerHandler;
import org.pragmaticminds.crunch.api.trigger.strategy.LambdaTriggerStrategy;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.GenericEvent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.booleanChannel;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.channel;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.longChannel;
import static org.pragmaticminds.crunch.api.trigger.strategy.TriggerStrategies.onBecomeTrue;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.07.2018
 */
public class TriggerEvaluationFunctionTest {
    private long timestamp = 123L;
    private Map<String, Value> valueMap = new HashMap<>();
    private GenericEvent resultEvent = new GenericEvent(timestamp, "testEventName", "testSource", valueMap);

    @Before
    public void setUp() throws Exception {
        valueMap.put("test1", Value.of(1L));
        valueMap.put("test2", Value.of(2L));
    }

    @Test
    public void processElementNotTriggered() {
        TriggerEvaluationFunction<GenericEvent> function = TriggerEvaluationFunction.<GenericEvent>builder()
                .withTriggerStrategy(new LambdaTriggerStrategy(
                        decisionBase -> false,
                        HashSet::new
                ))
            .withTriggerHandler(new TriggerHandler<GenericEvent>() {
                @Override public void handle(EvaluationContext<GenericEvent> context) {
                    context.collect(resultEvent);
                }

                @Override public Set<String> getChannelIdentifiers() {
                    return null;
                }
            })
                .build();

        Map<String, Value> values = new HashMap<>();
        TypedValues typedValues = new TypedValues("testSource", timestamp, values);
        List<GenericEvent> resultEventList;

        try {
            SimpleEvaluationContext<GenericEvent> context = new SimpleEvaluationContext<>(typedValues);
            resultEventList = context.getEvents();
            function.eval(context);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        assertTrue(resultEventList.isEmpty());
    }

    @Test
    public void processElementTriggeredNoResults() {
        TriggerHandler<GenericEvent> triggerHandler = Mockito.mock(TriggerHandler.class);

        TriggerEvaluationFunction<GenericEvent> function = TriggerEvaluationFunction.<GenericEvent>builder()
                .withTriggerStrategy(new LambdaTriggerStrategy(
                        decisionBase -> true,
                        HashSet::new
                ))
                .withTriggerHandler(triggerHandler)
                .build();
        Map<String, Value> values = new HashMap<>();
        TypedValues typedValues = new TypedValues("testSource", timestamp, values);
        List<GenericEvent> resultEventList;
        try {
            SimpleEvaluationContext<GenericEvent> context = new SimpleEvaluationContext<>(typedValues);
            resultEventList = context.getEvents();
            function.eval(context);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        Mockito.verify(triggerHandler, Mockito.times(1)).handle(Mockito.any());
        assertTrue(resultEventList.isEmpty());
    }

    @Test
    public void processElementTriggeredClone() {
        TriggerHandler<GenericEvent> triggerHandler = Mockito.mock(TriggerHandler.class);

        TriggerEvaluationFunction<GenericEvent> function = TriggerEvaluationFunction.<GenericEvent>builder()
                .withTriggerStrategy(new LambdaTriggerStrategy(
                        decisionBase -> true,
                        HashSet::new
                ))
                .withTriggerHandler(triggerHandler)
                .build();
        function = ClonerUtil.clone(function);

        Map<String, Value> values = new HashMap<>();
        TypedValues typedValues = new TypedValues("testSource", timestamp, values);
        List<GenericEvent> resultEventList;
        try {
            SimpleEvaluationContext<GenericEvent> context = new SimpleEvaluationContext<>(typedValues);
            resultEventList = context.getEvents();
            function.eval(context);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        assertTrue(resultEventList.isEmpty());
    }

    @Test
    public void processElementOneResult() {
        GenericEvent resultEvent = new GenericEvent(timestamp, "testEventName", "testSource");
        TriggerEvaluationFunction<GenericEvent> function = TriggerEvaluationFunction.<GenericEvent>builder()
                .withTriggerStrategy(new LambdaTriggerStrategy(
                        decisionBase -> true,
                        HashSet::new
                ))
            .withTriggerHandler(new TriggerHandler<GenericEvent>() {
                @Override public void handle(EvaluationContext<GenericEvent> context) {
                    context.collect(resultEvent);
                }

                @Override public Set<String> getChannelIdentifiers() {
                    return null;
                }
            })
                .build();
        Map<String, Value> values = new HashMap<>();
        TypedValues typedValues = new TypedValues("testSource", timestamp, values);
        List<GenericEvent> resultEventList;
        try {
            SimpleEvaluationContext<GenericEvent> context = new SimpleEvaluationContext<>(typedValues);
            resultEventList = context.getEvents();
            function.eval(context);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        assertEquals(1, resultEventList.size());
        assertTrue(resultEventList.contains(resultEvent));
    }

    @Test
    public void processElementManyResults() {
        TriggerEvaluationFunction<GenericEvent> function = TriggerEvaluationFunction.<GenericEvent>builder()
                .withTriggerStrategy(new LambdaTriggerStrategy(
                        decisionBase -> true,
                        HashSet::new
                ))
            .withTriggerHandler(new TriggerHandler<GenericEvent>() {
                @Override public void handle(EvaluationContext<GenericEvent> context) {
                    context.collect(resultEvent);
                    context.collect(resultEvent);
                    context.collect(resultEvent);
                }

                @Override public Set<String> getChannelIdentifiers() {
                    return null;
                }
            })
                .build();
        Map<String, Value> values = new HashMap<>();
        TypedValues typedValues = new TypedValues("testSource", timestamp, values);
        List<GenericEvent> resultEventList;
        try {
            SimpleEvaluationContext<GenericEvent> context = new SimpleEvaluationContext<>(typedValues);
            resultEventList = context.getEvents();
            function.eval(context);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        assertEquals(3, resultEventList.size());
        assertTrue(resultEventList.contains(resultEvent));
    }

    @Test
    public void processWithResultFilter() {
        TriggerEvaluationFunction<GenericEvent> function = TriggerEvaluationFunction.<GenericEvent>builder()
                .withTriggerStrategy(new LambdaTriggerStrategy(
                        decisionBase -> true,
                        HashSet::new
                ))
            .withTriggerHandler(new TriggerHandler<GenericEvent>() {
                @Override public void handle(EvaluationContext<GenericEvent> context) {
                    context.collect(resultEvent);
                    context.collect(resultEvent);
                    context.collect(resultEvent);
                }

                @Override public Set<String> getChannelIdentifiers() {
                    return null;
                }
            })
                .withFilter(new EventFilter<GenericEvent>() {
                    @Override
                    public boolean apply(GenericEvent event, MRecord values) {
                        return values.getString("val").equals("string");
                    }

                    @Override
                    public Collection<String> getChannelIdentifiers() {
                        return new ArrayList<>();
                    }
                })
                .build();
        Map<String, Value> values = new HashMap<>();
        values.put("val",Value.of("string"));
        TypedValues typedValues = new TypedValues("testSource", timestamp, values);
        List<GenericEvent> resultEventList;
        try {
            SimpleEvaluationContext<GenericEvent> context = new SimpleEvaluationContext<>(typedValues);
            resultEventList = context.getEvents();
            function.eval(context);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        assertEquals(3, resultEventList.size());
        assertTrue(resultEventList.contains(resultEvent));
    }

    @Test
    public void getChannelIdentifier() {
        TriggerEvaluationFunction<GenericEvent> function = TriggerEvaluationFunction.<GenericEvent>builder()
                .withTriggerStrategy(new LambdaTriggerStrategy(
                        record -> false,
                        () -> new HashSet<>(Collections.singletonList("test"))
                ))
                .withTriggerHandler((TriggerHandler<GenericEvent>) mock(TriggerHandler.class))
                .build();

        assertTrue(function.getChannelIdentifiers().contains("test"));
    }

    @Test
    public void processWithTriggerHandler() {
        /*
         * Prepare testing object
         */

        // create channel extractors
        MapExtractor extractor1 = Extractors.channelMapExtractor(
                channel("test1")
        );
        MapExtractor extractor2 = Extractors.channelMapExtractor(
                channel("test2")
        );

        // create TriggerHandler
        TriggerHandler<GenericEvent> triggerHandler = new GenericExtractorTriggerHandler("resultEvent", extractor1, extractor2);

        // create TriggerEvaluationFunction
        TriggerEvaluationFunction<GenericEvent> function = TriggerEvaluationFunction.<GenericEvent>builder()
                .withTriggerStrategy(new LambdaTriggerStrategy(
                        decisionBase -> true,
                        HashSet::new
                ))
                .withTriggerHandler(triggerHandler)
                .build();

        /*
         * Prepare test values
         */
        // prepare MRecord
        TypedValues typedValues = new TypedValues("testSource", timestamp, valueMap);

        // prepare EvaluationContext
        SimpleEvaluationContext context = new SimpleEvaluationContext(typedValues);

        /*
         * Execute functionality
         */
        function.eval(context);

        /*
         * Check results
         */
        List<GenericEvent>resultEventList = context.getEvents();

        // check size
        Assert.assertEquals(1, resultEventList.size());

        // check contents
        GenericEvent event = resultEventList.get(0);
        Assert.assertEquals(1L, (long)event.getParameter("test1").getAsLong());
        Assert.assertEquals(2L, (long)event.getParameter("test2").getAsLong());
    }

    @Test
    public void getAllChannelIdentifiersAlsoFromExtractor() {
        TriggerEvaluationFunction<GenericEvent> function = TriggerEvaluationFunction.<GenericEvent>builder()
            .withTriggerStrategy(onBecomeTrue(booleanChannel("signal_0")))
            .withTriggerHandler(new GenericExtractorTriggerHandler("my_event", Extractors.channelMapExtractor(longChannel("channel1"))))
            .build();

        final Set<String> channelIdentifiers = function.getChannelIdentifiers();

        System.out.println(function.getChannelIdentifiers());

        assertEquals(2, channelIdentifiers.size());
    }
}