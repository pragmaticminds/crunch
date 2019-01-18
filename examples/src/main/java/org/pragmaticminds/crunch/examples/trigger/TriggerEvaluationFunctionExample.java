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

package org.pragmaticminds.crunch.examples.trigger;

import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.trigger.TriggerEvaluationFunction;
import org.pragmaticminds.crunch.api.trigger.comparator.Suppliers;
import org.pragmaticminds.crunch.api.trigger.extractor.Extractors;
import org.pragmaticminds.crunch.api.trigger.filter.EventFilter;
import org.pragmaticminds.crunch.api.trigger.handler.GenericExtractorTriggerHandler;
import org.pragmaticminds.crunch.api.trigger.handler.TriggerHandler;
import org.pragmaticminds.crunch.api.trigger.strategy.LambdaTriggerStrategy;
import org.pragmaticminds.crunch.api.trigger.strategy.TriggerStrategy;
import org.pragmaticminds.crunch.events.GenericEvent;
import org.pragmaticminds.crunch.examples.SimpleEvaluationExample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * This Class is a example for a simple evaluation crunch processing, where a sum over all channel values is created and
 * written into the resulting {@link GenericEvent}.
 *
 * @author erwin.wagasow
 * created by erwin.wagasow on 18.01.2019
 */
public class TriggerEvaluationFunctionExample extends SimpleEvaluationExample {

    private static final Logger logger = LoggerFactory.getLogger(TriggerEvaluationFunctionExample.class);

    public static void main(String[] args) {
        logger.info("application started");
        TriggerEvaluationFunctionExample example = new TriggerEvaluationFunctionExample();
        example.getExecutor().run();
    }


    /**
     * Generates the main part of this example, the {@link TriggerEvaluationFunction} of {@link GenericEvent} type.
     *
     * @param channels all relevant for the internal processing of the {@link EvaluationFunction}.
     * @return new instance of {@link TriggerEvaluationFunction}.
     */
    @Override
    protected EvaluationFunction<GenericEvent> generateEvaluationFunction(Set<String> channels) {
        TriggerStrategy triggerStrategy = generateTriggerStrategy(channels);
        TriggerHandler<GenericEvent> triggerHandler = generateTriggerHandler(channels);
        return TriggerEvaluationFunction.<GenericEvent>builder()
                // decides if the condition of the EvaluationFunction is met
                .withTriggerStrategy(triggerStrategy)
                // creates a resulting event for the triggering of the EvaluationFunction
                .withTriggerHandler(triggerHandler)
                .withFilter(generateFilter())
                .build();
    }

    /**
     * Generates the {@link TriggerStrategy} that is used inside the {@link TriggerEvaluationFunction} to determine
     * whether the {@link EvaluationFunction} is to be triggered.
     *
     * @param   channels all relevant for the internal processing of the {@link EvaluationFunction}.
     * @return  new instance of TriggerStrategy implemented as lambdas.
     */
    public TriggerStrategy generateTriggerStrategy(Set<String> channels) {
        HashSet<String> channels1 = new HashSet<>(channels);
        return new LambdaTriggerStrategy(
                // definition of the trigger -> only triggers if the first channel in the set has a value higher that 1.
                mRecord -> {
                    try {
                        return mRecord.getDouble(mRecord.getChannels().iterator().next()) > 1D;
                    } catch (Exception e) {
                        logger.warn("reading record failed!", e);
                        return false;
                    }
                },
                // to determine which records are relevant for this EvaluationFunction, the EvaluationFunction must know
                // it's channels
                () -> new HashSet<String>(channels1)
        );
    }

    /**
     * Generates the handler that is triggered by the TriggerStrategy. It generates the resulting {@link GenericEvent}.
     *
     * @param channels  all relevant for the internal processing of the {@link EvaluationFunction}.
     * @return          new instance of {@link TriggerHandler}.
     */
    public GenericExtractorTriggerHandler generateTriggerHandler(Set<String> channels) {
        HashSet<String> channels1 = new HashSet<>(channels);
        return new GenericExtractorTriggerHandler("resultEvent", Extractors.channelMapExtractor(Suppliers.ChannelExtractors.doubleChannel(channels1.iterator().next())));
    }

    /**
     * Defines the filter, that only passes relevant resulting {@link GenericEvent}s.
     *
     * @return null : no need of a {@link EventFilter} in this example.
     */
    public EventFilter<GenericEvent> generateFilter() {
        // no use of filter in this example
        return null;
    }
}
