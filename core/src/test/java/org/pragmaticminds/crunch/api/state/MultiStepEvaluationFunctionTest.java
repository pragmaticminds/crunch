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

package org.pragmaticminds.crunch.api.state;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.pragmaticminds.crunch.api.exceptions.OverallTimeoutException;
import org.pragmaticminds.crunch.api.exceptions.StepTimeoutException;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.LambdaEvaluationFunction;
import org.pragmaticminds.crunch.api.pipe.SimpleEvaluationContext;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.events.GenericEvent;
import org.pragmaticminds.crunch.events.GenericEventBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/** *
 * @author Erwin Wagasow
 * @author kerstin
 * Created by Erwin Wagasow on 09.08.2018
 */
public class MultiStepEvaluationFunctionTest implements Serializable {

  private ErrorExtractor<GenericEvent> errorExtractor = new ErrorExtractor<GenericEvent>() {
    @Override public void process(Map<String, GenericEvent> events, Exception ex, EvaluationContext<GenericEvent> context) {
      context.collect(new GenericEvent(0L, ex.getClass().getName(), ""));
    }

    @Override public Set<String> getChannelIdentifiers() {
      return Collections.emptySet();
    }
  };
  private EvaluationCompleteExtractor<GenericEvent> evaluationCompleteExtractor = new EvaluationCompleteExtractor<GenericEvent>() {
    @Override public void process(Map<String, GenericEvent> events, EvaluationContext<GenericEvent> context) {
      for (Map.Entry<String, GenericEvent> stringEventEntry : events.entrySet()) {
        Map.Entry<String, GenericEvent> entry = stringEventEntry;
        context.collect(entry.getValue());
      }
    }

    @Override public Set<String> getChannelIdentifiers() {
      return Collections.emptySet();
    }
    };
    private GenericEvent successEvent = GenericEventBuilder.anEvent()
            .withTimestamp(System.currentTimeMillis())
            .withSource("test")
            .withEvent("success")
            .withParameter("string", "string")
            .build();
    private TypedValues typedValues = TypedValues.builder().source("test").values(Collections.emptyMap()).build();

    @Test
    public void evalDefaultTimeOut_noTimeOutOccurs() { // -> processing should be successful with no timers set
        // create instance of the MultiStepEvaluationFunction with parameters for this test
        MultiStepEvaluationFunction stateMachine = MultiStepEvaluationFunction.<GenericEvent>builder()
                .addEvaluationFunction(
                        new LambdaEvaluationFunction<GenericEvent>(
                                ctx -> ctx.collect(successEvent),
                                () -> new HashSet<>(Arrays.asList("string"))
                        ),
                        "success1",
                        10
                )
                .addEvaluationFunction(
                        new LambdaEvaluationFunction<GenericEvent>(
                                ctx -> ctx.collect(successEvent),
                                () -> new HashSet<>(Arrays.asList("string"))
                        ),
                        "success2",
                        10
                )
                .withEvaluationCompleteExtractor(evaluationCompleteExtractor)
                .withErrorExtractor(errorExtractor)
                .build();

        SimpleEvaluationContext<GenericEvent> context;
        context = new SimpleEvaluationContext<>(typedValues);

        stateMachine.eval(context);

        context = new SimpleEvaluationContext<>(
                new TypedValues(
                        "",
                        typedValues.getTimestamp() + 9,
                        typedValues.getValues()
                )
        );
        stateMachine.eval(context);

        assertNotNull(context.getEvents());
        assertEquals(2, context.getEvents().size());
        for (GenericEvent event : context.getEvents()) {
            assertEquals(successEvent, event);
        }
    }

    @Test
    public void eval_withStepTimeout() { // -> a state timeout should be raised
        MultiStepEvaluationFunction<GenericEvent> stateMachine = MultiStepEvaluationFunction.<GenericEvent>builder()
                .withErrorExtractor(errorExtractor)
                .withEvaluationCompleteExtractor(evaluationCompleteExtractor)
                .addEvaluationFunction(
                        new LambdaEvaluationFunction<>(
                                ctx -> ctx.collect(successEvent),
                                () -> new HashSet<>(Arrays.asList("string"))
                        ),
                        "success",
                        10
                )
                .addEvaluationFunction(
                        new LambdaEvaluationFunction<>(
                                ctx -> { /* do nothing */ },
                                () -> new HashSet<>(Arrays.asList("string"))
                        ),
                        "timeout"
                )
                .build();

        SimpleEvaluationContext context = new SimpleEvaluationContext(typedValues);
        stateMachine.eval(context); // second time, so that error can be passed
        // now the second call should trigger a state timeout because it's timestamp is 100 but can be 10 at maximum
        SimpleEvaluationContext<GenericEvent> context2 = new SimpleEvaluationContext<>(new TypedValues("", typedValues.getTimestamp() + 100, typedValues.getValues()));
        stateMachine.eval(context2);
        assertTrue(context.getEvents().isEmpty());
        assertEquals(1, context2.getEvents().size());
        assertEquals(StepTimeoutException.class.getName(), context2.getEvents().get(0).getEventName());
    }

    @Test
    public void eval_withOverallTimeout() { // -> a overall timeout should be raised
        MultiStepEvaluationFunction<GenericEvent> stateMachine = MultiStepEvaluationFunction.<GenericEvent>builder()
                .withErrorExtractor(errorExtractor)
                .withOverallTimeoutMs(10)
                .withEvaluationCompleteExtractor(evaluationCompleteExtractor)
                .addEvaluationFunction(
                        new LambdaEvaluationFunction<>(
                                ctx -> ctx.collect(successEvent),
                                () -> new HashSet<>(ImmutableList.of("string"))
                        ),
                        "success1"
                )
                .addEvaluationFunction(
                        new LambdaEvaluationFunction<>(
                                ctx -> { /* do nothing */ },
                                () -> new HashSet<>(ImmutableList.of("string"))
                        ),
                        "timeout1"
                )
                .addEvaluationFunction(
                        new LambdaEvaluationFunction<>(
                                ctx -> { /* do nothing */ },
                                () -> new HashSet<>(ImmutableList.of("string"))
                        ),
                        "timeout2"
                )
                .build();
        SimpleEvaluationContext context = new SimpleEvaluationContext(typedValues);
        stateMachine.eval(context);
        SimpleEvaluationContext context2 = new SimpleEvaluationContext(new TypedValues("", typedValues.getTimestamp()+5, typedValues.getValues()));
        stateMachine.eval(context2);
        SimpleEvaluationContext<GenericEvent> context3 = new SimpleEvaluationContext<>(new TypedValues("", typedValues.getTimestamp() + 11, typedValues.getValues()));
        stateMachine.eval(context3);
        assertTrue(context.getEvents().isEmpty());
        assertTrue(context2.getEvents().isEmpty());
        assertEquals(1, context3.getEvents().size());
        assertEquals(OverallTimeoutException.class.getName(), context3.getEvents().get(0).getEventName());
    }

    @Test
    public void test_builder() {
        MultiStepEvaluationFunction.Builder<Serializable> builder = MultiStepEvaluationFunction.<Serializable>builder()
                .withErrorExtractor(new ErrorExtractor<Serializable>() {
                    @Override public void process(Map<String, Serializable> events, Exception ex, EvaluationContext<Serializable> context) {}
                    @Override public Set<String> getChannelIdentifiers() {
                    return null;
                  }
                })
                .withEvaluationCompleteExtractor(new EvaluationCompleteExtractor<Serializable>() {
                    @Override public void process(Map<String, Serializable> events, EvaluationContext<Serializable> context) {}
                    @Override public Set<String> getChannelIdentifiers() {
                        return null;
                    }
                })
                .withOverallTimeoutMs(0L)
                .addEvaluationFunction(new LambdaEvaluationFunction<>(
                                ctx -> { /* do nothing */ },
                                () -> new HashSet<>(ImmutableList.of("string"))
                        ),""
                ).addEvaluationFunction(new LambdaEvaluationFunction<>(
                                ctx -> { /* do nothing */ },
                                () -> new HashSet<>(ImmutableList.of("string"))
                        ),"",0L
                ).addEvaluationFunctionFactory(
                        CloneStateEvaluationFunctionFactory.<Serializable>builder()
                                .withPrototype(new LambdaEvaluationFunction<Serializable>(
                                        ctx -> { /* do nothing */ },
                                        () -> new HashSet<>(ImmutableSet.of("string"))
                                )).build(),
                        ""
                ).addEvaluationFunctionFactory(
                        CloneStateEvaluationFunctionFactory.<Serializable>builder()
                                .withPrototype(new LambdaEvaluationFunction<>(
                                        ctx -> { /* do nothing */ },
                                        () -> new HashSet<>(Arrays.asList("string"))
                                )).build(),
                        "",
                        0L
                )
                ;

        builder.build();
    }

    @Test
    public void getChannelIdentifiers() {
        MultiStepEvaluationFunction<GenericEvent> function = MultiStepEvaluationFunction.<GenericEvent>builder()
                .withErrorExtractor((ErrorExtractor<GenericEvent>) mock(ErrorExtractor.class))
                .withEvaluationCompleteExtractor((EvaluationCompleteExtractor<GenericEvent>) mock(EvaluationCompleteExtractor.class))
                .withOverallTimeoutMs(0L)
                .addEvaluationFunction(new LambdaEvaluationFunction<>(
                        context -> {},
                        () -> new HashSet<>(Arrays.asList("1", "2", "3"))
                ), "step1")
                .addEvaluationFunction(new LambdaEvaluationFunction<>(
                        context -> {},
                        () -> new HashSet<>(Arrays.asList("3", "4", "5"))
                ), "step2")
                .build();

        List<String> channelIdentifiers = new ArrayList<>(function.getChannelIdentifiers());
        assertEquals(5, channelIdentifiers.size());
        assertTrue(channelIdentifiers.contains("1"));
        assertTrue(channelIdentifiers.contains("2"));
        assertTrue(channelIdentifiers.contains("3"));
        assertTrue(channelIdentifiers.contains("4"));
        assertTrue(channelIdentifiers.contains("5"));
    }
}
