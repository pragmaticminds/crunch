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
import org.pragmaticminds.crunch.api.trigger.comparator.Suppliers;
import org.pragmaticminds.crunch.api.trigger.strategy.TriggerStrategies;
import org.pragmaticminds.crunch.api.trigger.strategy.TriggerStrategy;
import org.pragmaticminds.crunch.examples.sources.ValuesGenerator;

import java.util.*;

/**
 * In this class are examples of {@link TriggerStrategy} usages.
 *
 * @author erwin.wagasow
 * created by erwin.wagasow on 21.01.2019
 */
public class TriggerStrategiesExamples {

    private static final String CHANNEL_NAME_PREFIX = "channel_";
    private static final String CHANNEL_DOUBLE_NEGATIVE = CHANNEL_NAME_PREFIX + "doubleNegative";
    private static final String CHANNEL_LONG = CHANNEL_NAME_PREFIX + "long";
    private static final String CHANNEL_BOOLEAN_FALSE = CHANNEL_NAME_PREFIX + "booleanFalse";
    private static final String CHANNEL_BOOLEAN_TRUE = CHANNEL_NAME_PREFIX + "booleanTrue";
    private static final String CHANNEL_DOUBLE = CHANNEL_NAME_PREFIX + "double";

    /** hidden constructor */
    private TriggerStrategiesExamples() {
        /* do nothing */
    }

    /** In this case the {@link EvaluationFunction} is always triggered, when a new record arrives. */
    public static class TriggerStrategyAlwaysExample extends TriggerEvaluationFunctionExample {
        public static void main(String[] args) {
            TriggerStrategyAlwaysExample example = new TriggerStrategyAlwaysExample();
            example.getExecutor().run();
        }

        /** Definition of the example TriggerStrategy */
        @Override
        public TriggerStrategy generateTriggerStrategy(Set<String> channels) {
            return TriggerStrategies.always();
        }
    }

    /** In this case the EvaluationFunction is only triggered, when the set Channel has a value that is not null. */
    public static class TriggerStrategyNotNullExample extends TriggerEvaluationFunctionExample {
        public static void main(String[] args) {
            TriggerStrategyNotNullExample example = new TriggerStrategyNotNullExample();
            example.getExecutor().run();
        }

        /** create different example channel values */
        @Override
        public List<Map<String, Object>> getInData() {
            return ValuesGenerator.generateMixedData(100, CHANNEL_NAME_PREFIX);
        }

        /** Definition of the example TriggerStrategy */
        @Override
        public TriggerStrategy generateTriggerStrategy(Set<String> channels) {
            return TriggerStrategies.onNotNull(Suppliers.ChannelExtractors.doubleChannel(CHANNEL_DOUBLE));
        }
    }

    /** In this case the EvaluationFunction is only triggered, when the set Channel has the boolean value true. */
    public static class TriggerStrategyOnTrueExample extends TriggerEvaluationFunctionExample {
        public static void main(String[] args) {
            TriggerStrategyOnTrueExample example = new TriggerStrategyOnTrueExample();
            example.getExecutor().run();
        }

        /** create different example channel values */
        @Override
        public List<Map<String, Object>> getInData() {
            return ValuesGenerator.generateMixedData(100, CHANNEL_NAME_PREFIX);
        }

        /** Definition of the example TriggerStrategy */
        @Override
        public TriggerStrategy generateTriggerStrategy(Set<String> channels) {
            return TriggerStrategies.onTrue(Suppliers.ChannelExtractors.booleanChannel(CHANNEL_BOOLEAN_TRUE));
        }
    }

    /** In this case the EvaluationFunction is only triggered, when the set Channel has the boolean value true. */
    public static class TriggerStrategyOnBecomeFalseExample extends TriggerEvaluationFunctionExample {
        public static void main(String[] args) {
            TriggerStrategyOnBecomeFalseExample example = new TriggerStrategyOnBecomeFalseExample();
            example.getExecutor().run();
        }

        /** create different example channel values */
        @Override
        public List<Map<String, Object>> getInData() {
            return ValuesGenerator.generateMixedData(100, CHANNEL_NAME_PREFIX);
        }

        /** Definition of the example TriggerStrategy */
        @Override
        public TriggerStrategy generateTriggerStrategy(Set<String> channels) {
            return TriggerStrategies.onBecomeFalse(Suppliers.ChannelExtractors.booleanChannel(CHANNEL_BOOLEAN_FALSE));
        }
    }

    /** In this case the EvaluationFunction is only triggered, when the set Channel has the boolean value true. */
    public static class TriggerStrategyOnChangeGreaterThanExample extends TriggerEvaluationFunctionExample {
        private transient List<Map<String, Object>> inData;

        public static void main(String[] args) {
            TriggerStrategyOnChangeGreaterThanExample example = new TriggerStrategyOnChangeGreaterThanExample();
            example.getExecutor().run();
        }

        /** create different example channel values */
        @Override
        public List<Map<String, Object>> getInData() {
            if(inData == null){
                inData = ValuesGenerator.generateMixedData(100, CHANNEL_NAME_PREFIX);
            }
            return inData;
        }

        /** Definition of the example TriggerStrategy */
        @Override
        public TriggerStrategy generateTriggerStrategy(Set<String> channels) {
            return TriggerStrategies.onChange(
                    Suppliers.Comparators.greaterThan(
                            // the one that should be greater
                            Suppliers.ChannelExtractors.longChannel(CHANNEL_LONG),
                            // the one that should be smaller
                            Suppliers.ChannelExtractors.doubleChannel(CHANNEL_DOUBLE_NEGATIVE)
                    )
            );
        }
    }
}
