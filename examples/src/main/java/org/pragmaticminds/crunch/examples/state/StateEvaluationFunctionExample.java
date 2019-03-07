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

package org.pragmaticminds.crunch.examples.state;

import com.google.common.collect.ImmutableMap;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.state.ErrorExtractor;
import org.pragmaticminds.crunch.api.state.EvaluationCompleteExtractor;
import org.pragmaticminds.crunch.api.state.EvaluationFunctionStateFactory;
import org.pragmaticminds.crunch.api.state.MultiStepEvaluationFunction;
import org.pragmaticminds.crunch.api.trigger.TriggerEvaluationFunction;
import org.pragmaticminds.crunch.api.trigger.comparator.Suppliers;
import org.pragmaticminds.crunch.api.trigger.extractor.MapExtractor;
import org.pragmaticminds.crunch.api.trigger.handler.GenericExtractorTriggerHandler;
import org.pragmaticminds.crunch.api.trigger.strategy.TriggerStrategies;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.GenericEvent;
import org.pragmaticminds.crunch.events.GenericEventBuilder;
import org.pragmaticminds.crunch.examples.SimpleEvaluationExample;
import org.pragmaticminds.crunch.examples.sources.ValuesGenerator;

import java.time.Instant;
import java.util.*;

/**
 * This class demonstrates the use of the {@link MultiStepEvaluationFunction}.
 * 3 Steps are implemented.
 *
 * @author erwin.wagasow
 * created by erwin.wagasow on 22.01.2019
 */
public class StateEvaluationFunctionExample extends SimpleEvaluationExample {
    private static final String CHANNEL_LONG = "channel_long";
    private transient List<Map<String, Object>> inData;

    public static void main(String[] args) {
        StateEvaluationFunctionExample example = new StateEvaluationFunctionExample();
        example.getExecutor().run();
    }

    /**
     * Prepare test data.
     *
     * @return data to generate the test records.
     */
    @Override
    public List<Map<String, Object>> getInData() {
        if(inData == null){
            inData = ValuesGenerator.generateMixedData(100, "channel_");
        }
        return inData;
    }

    /**
     * In here the {@link MultiStepEvaluationFunction} itself is defined.
     *
     * @param channels all relevant channels for processing.
     * @return a new instance of {@link MultiStepEvaluationFunction}.
     */
    @Override
    public EvaluationFunction<GenericEvent> generateEvaluationFunction(Set<String> channels) {
        return MultiStepEvaluationFunction

                // create a builder
                .<GenericEvent>builder()

                // add the first step EvaluationFunction inside the MultiStepEvaluationFunction
                .addEvaluationFunction(generateStep1EvaluationFunction(), "step1")

                // add the second step EvaluationFunction inside the MultiStepEvaluationFunction
                .addEvaluationFunctionFactory(generateStep2EvaluationFunctionFactory(), "step2")

                // add the third step EvaluationFunction inside the MultiStepEvaluationFunction
                .addEvaluationFunctionFactory(generateStep3EvaluationFunction(), "step3", 10_000L)

                // set the extractor, which creates the final event out of the events of all steps
                .withEvaluationCompleteExtractor(
                        new EvaluationCompleteExtractor<GenericEvent>() {
                            @Override
                            public void process(Map<String, GenericEvent> events, EvaluationContext<GenericEvent> context) {
                                context.collect(GenericEventBuilder.anEvent()
                                        .withEvent("MultiStepFunctionComplete")
                                        .withSource("test")
                                        .withTimestamp(Instant.now().toEpochMilli())
                                        .withParameter("step1Complete", Value.of(true))
                                        .withParameter("step2Complete", Value.of(true))
                                        .withParameter("step3Complete", Value.of(true))
                                        .build());
                            }

                            @Override
                            public Set<String> getChannelIdentifiers() {
                                return Collections.singleton(CHANNEL_LONG);
                            }
                        }
                )

                // set the Error case extractor, which creates an error event
                .withErrorExtractor(new ErrorExtractor<GenericEvent>() {
                    @Override
                    public void process(Map<String, GenericEvent> events, Exception ex, EvaluationContext<GenericEvent> context) {
                        context.collect(GenericEventBuilder.anEvent()
                                .withEvent("MultiStepFunctionComplete")
                                .withSource("test")
                                .withParameter("Error", Value.of(true))
                                .build());
                    }
                    @Override
                    public Set<String> getChannelIdentifiers() {
                        return Collections.singleton(CHANNEL_LONG);
                    }
                })

                // build the MultiStepEvaluationFunction
                .build();
    }

    /**
     * Creates the {@link EvaluationFunction} for the first Step in the processing.
     *
     * @return a new instance of {@link EvaluationFunction}.
     */
    public EvaluationFunction<GenericEvent> generateStep1EvaluationFunction(){
        return TriggerEvaluationFunction.<GenericEvent>builder()
                .withTriggerStrategy(
                        TriggerStrategies.onTrue(
                                Suppliers.Comparators.equals(
                                        2L,
                                        Suppliers.ChannelExtractors.longChannel(CHANNEL_LONG)
                                )
                        )
                )
                .withTriggerHandler(
                        new GenericExtractorTriggerHandler(
                                "step1",
                                new LocalMapExtractor("step1Done", Value.of(true))
                        )
                )
                .build();
    }

    /**
     * Creates the {@link EvaluationFunctionStateFactory} for the second Step in the processing.
     *
     * @return a new instance of {@link EvaluationFunctionStateFactory}.
     */
    public EvaluationFunctionStateFactory<GenericEvent> generateStep2EvaluationFunctionFactory() {
        return new EvaluationFunctionStateFactory<GenericEvent>() {
            @Override
            public EvaluationFunction<GenericEvent> create() {
                return TriggerEvaluationFunction.<GenericEvent>builder()
                        .withTriggerStrategy(
                                TriggerStrategies.onTrue(
                                        Suppliers.Comparators.equals(
                                                3L,
                                                Suppliers.ChannelExtractors.longChannel(CHANNEL_LONG)
                                        )
                                )
                        )
                        .withTriggerHandler(
                                new GenericExtractorTriggerHandler(
                                        "step2",
                                        new LocalMapExtractor("step2Done", Value.of(true))
                                )
                        )
                        .build();
            }

            @Override
            public Collection<String> getChannelIdentifiers() {
                return Collections.singleton(CHANNEL_LONG);
            }
        };
    }

    /**
     * Creates the {@link EvaluationFunctionStateFactory} for the third Step in the processing.
     *
     * @return a new instance of {@link EvaluationFunctionStateFactory}.
     */
    private EvaluationFunctionStateFactory<GenericEvent> generateStep3EvaluationFunction() {
        return new EvaluationFunctionStateFactory<GenericEvent>() {
            @Override
            public EvaluationFunction<GenericEvent> create() {
                return TriggerEvaluationFunction.<GenericEvent>builder()
                        .withTriggerStrategy(
                                TriggerStrategies.onTrue(
                                        Suppliers.Comparators.equals(
                                                4L,
                                                Suppliers.ChannelExtractors.longChannel(CHANNEL_LONG)
                                        )
                                )
                        )
                        .withTriggerHandler(
                                new GenericExtractorTriggerHandler(
                                        "step3",
                                        new LocalMapExtractor("step3Done", Value.of(true))
                                )
                        )
                        .build();
            }

            @Override
            public Collection<String> getChannelIdentifiers() {
                return Collections.singleton(CHANNEL_LONG);
            }
        };
    }

    /**
     * Local implementation of the {@link MapExtractor} which adds one parameter for the resulting {@link GenericEvent}.
     */
    public static class LocalMapExtractor implements MapExtractor {
        String key;
        Value value;

        public LocalMapExtractor(String key, Value value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public Map<String, Value> extract(EvaluationContext context) {
            return ImmutableMap.of(key, value);
        }
        @Override
        public Set<String> getChannelIdentifiers() {
            return Collections.singleton(CHANNEL_LONG);
        }
    }
}
