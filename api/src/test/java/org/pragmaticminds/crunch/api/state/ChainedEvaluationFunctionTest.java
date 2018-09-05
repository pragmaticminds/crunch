package org.pragmaticminds.crunch.api.state;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.pipe.SimpleEvaluationContext;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.Event;
import org.pragmaticminds.crunch.events.EventBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * This test covers 4 cases of processing {@link TypedValues} by a {@link ChainedEvaluationFunction}.
 * Case 1: no timeouts are set
 * Case 2: timeouts are set but should not be raised while processing
 * Case 3: timeouts are set and a state timeout should be raised
 * Case 4: timeouts are set and a overall timeout should be raised
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 09.08.2018
 */
public class ChainedEvaluationFunctionTest implements Serializable {

    private List<StateConfig> stateFactories;
    private List<StateConfig> stateFactoriesWithStateTimeoutsRaised;
    private List<StateConfig> stateFactoriesWithOverallTimeoutsRaised;
    private StateErrorExtractor                                        errorExtractor;
    private ChainProcessingCompleteExtractor                           stateCompleteExtractor;
    private EvaluationFunction                                         prototypeFunction;
    private Long                                                       overallTimeout;
    private Event                                                      errorStateEvent;
    private Event                                                      errorOverallEvent;
    private Event                                                      errorEvent;
    private Event                                                      completeEvent;
    private Event                                                      event;
    private TypedValues                                                typedValues1;

    @Before
    public void setUp() {

        // prepare all values

        HashMap<String, Value> values = new HashMap<>();
        values.put("test", Value.of("test"));

        typedValues1 = TypedValues.builder().source("test").timestamp(System.currentTimeMillis()).values(values).build();
        typedValues1 = TypedValues.builder().source("test").timestamp(System.currentTimeMillis()+100).values(values).build();

        // this Event is expected to be the result on timeouts
        errorStateEvent = EventBuilder.anEvent()
                .withTimestamp(System.currentTimeMillis())
                .withSource("test")
                .withEvent("success")
                .withParameter("type", "Error")
                .build();

        // this Event is expected to be the result on timeouts
        errorOverallEvent = EventBuilder.anEvent()
                .withTimestamp(System.currentTimeMillis())
                .withSource("test")
                .withEvent("success")
                .withParameter("type", "Error")
                .build();

        // this Event is expected to be the result on timeouts
        errorEvent = EventBuilder.anEvent()
                .withTimestamp(System.currentTimeMillis())
                .withSource("test")
                .withEvent("success")
                .withParameter("type", "Error")
                .build();

        // this Event is expected to be the result on successfull processing
        completeEvent = EventBuilder.anEvent()
                .withTimestamp(System.currentTimeMillis())
                .withSource("test")
                .withEvent("success")
                .withParameter("type", "Complete")
                .build();

        // this Event is always given back as result from inner EvaluationFunctions
        event = EventBuilder.anEvent()
                .withTimestamp(System.currentTimeMillis())
                .withSource("test")
                .withEvent("success")
                .withParameter("type", "Default")
                .build();

        // for successful processing
        long stateTimeout = 100L;
        overallTimeout = 10000L;
        // for timeouts raised
        long stateTimeout10ms = 10L;

        // always successful processing EvaluationFunction
        prototypeFunction = new EvaluationFunction() {
            @Override
            public void eval(EvaluationContext ctx) {
                ctx.collect(event);
            }
        };

        // no resulting Event
        EvaluationFunction prototypeFunctionNoResult = new EvaluationFunction() {
            @Override
            public void eval(EvaluationContext ctx) {
                /* do nothing */
            }
        };

        // EvaluationFunctionStateFactories for the successful processing without any timeout settings
        stateFactories = new ArrayList<>();
        CloneStateEvaluationFunctionFactory cloneFactory = CloneStateEvaluationFunctionFactory.builder()
                .withPrototype(prototypeFunction)
                .build();
        StateConfig tuple = new StateConfig("alias", cloneFactory, 0L);
        // add 4 times the same EvaluationFunction
        stateFactories.add(tuple);
        stateFactories.add(tuple);
        stateFactories.add(tuple);
        stateFactories.add(tuple);

        // EvaluationFunctionStateFactories for the failed processing on stateTimeout raised
        stateFactoriesWithStateTimeoutsRaised = new ArrayList<>();
        CloneStateEvaluationFunctionFactory cloneFactoryWithStateTimeoutsRaised = CloneStateEvaluationFunctionFactory.builder()
                .withPrototype(prototypeFunctionNoResult)
                .build();
        // add single EvaluationFunctionStateFactory
        stateFactoriesWithStateTimeoutsRaised.add(new StateConfig("alias", cloneFactoryWithStateTimeoutsRaised, stateTimeout10ms));

        // EvaluationFunctionStateFactories for the failed processing on overallTimeout raised
        stateFactoriesWithOverallTimeoutsRaised = new ArrayList<>();
        CloneStateEvaluationFunctionFactory cloneFactoryWithOverallTimeoutsRaised = CloneStateEvaluationFunctionFactory.builder()
                .withPrototype(prototypeFunctionNoResult)
                .build();
        // add single EvaluationFunctionStateFactory
        stateFactoriesWithOverallTimeoutsRaised.add(new StateConfig("alias", cloneFactoryWithOverallTimeoutsRaised, stateTimeout));

        // extractor on timeouts and exceptions
        errorExtractor = (StateErrorExtractor) (events, ex, context) -> context.collect(errorEvent);

        // extractor on successful processing
        stateCompleteExtractor = (ChainProcessingCompleteExtractor) (events, context) -> context.collect(completeEvent);
    }

    @Test
    public void evalSimpleNoTimeouts() { // -> processing should be successful with no timers set
        // create instance of the ChainedEvaluationFunction with parameters for this test
        ChainedEvaluationFunction.Builder builder = ChainedEvaluationFunction.builder()
                .withErrorExtractor(errorExtractor)
                .withStateCompleteExtractor(stateCompleteExtractor);
        for (StateConfig stateFactory : stateFactories) {
            builder.addEvaluationFunction(stateFactory.getFactory().create(), stateFactory.getStateAlias(),
                    stateFactory.getStateTimeout());
        }

        ChainedEvaluationFunction stateMachine = builder.build();

                SimpleEvaluationContext context;
        for (int i = 0; i < 10; i++) { // do it 10 times, to make sure nothing is out of order
            for (int j = 0; j < 3; j++) {
                // prepare
                context= new SimpleEvaluationContext(typedValues1);
                // execute
                stateMachine.eval(context);
                // check
                Assert.assertEquals(0, context.getEvents().size());
            }
            // last state
            // prepare
            context = new SimpleEvaluationContext(typedValues1);
            // execute
            stateMachine.eval(context);
            // check
            Assert.assertNotNull(context.getEvents());
            Assert.assertEquals(1, context.getEvents().size());
            Assert.assertEquals(completeEvent, context.getEvents().get(0));
        }
    }

    @Test
    public void evalSimpleWithTimeouts() { // -> processing should be successful while having set up timers
        ChainedEvaluationFunction.Builder builder = ChainedEvaluationFunction.builder()
                .withErrorExtractor(errorExtractor)
                .withOverallTimeoutMs(overallTimeout)
                .withStateCompleteExtractor(stateCompleteExtractor);
        for (StateConfig stateFactory : stateFactories) {
            builder.addEvaluationFunction(stateFactory.getFactory().create(), stateFactory.getStateAlias(),
                    stateFactory.getStateTimeout());
        }

        ChainedEvaluationFunction statemachineWithTimeouts = builder.build();

        SimpleEvaluationContext context;
        for (int i = 0; i < 10; i++) { // do it 10 times, to make sure nothing is out of order
            for (int j = 0; j < 3; j++) {
                // prepare
                context = new SimpleEvaluationContext(typedValues1);
                // execute
                statemachineWithTimeouts.eval(context);
                // check
                Assert.assertEquals(0, context.getEvents().size());
            }
            // last state
            // prepare
            context = new SimpleEvaluationContext(typedValues1);
            // execute
            statemachineWithTimeouts.eval(context);
            // check
            Assert.assertNotNull(context.getEvents());
            Assert.assertEquals(1, context.getEvents().size());
            Assert.assertEquals(completeEvent, context.getEvents().get(0));
        }
    }

    @Test
    public void evalWithStateTimeoutRaised() { // -> a state timeout should be raised
        StateErrorExtractor errorStateExtractor = (StateErrorExtractor) (events, ex, context) -> context.collect(
                errorStateEvent);

        ChainedEvaluationFunction.Builder builder = ChainedEvaluationFunction.builder()
                .withErrorExtractor(errorStateExtractor)
                .withOverallTimeoutMs(overallTimeout)
                .withStateCompleteExtractor(stateCompleteExtractor);
        for (StateConfig stateFactory : stateFactoriesWithStateTimeoutsRaised) {
            builder.addEvaluationFunction(stateFactory.getFactory().create(), stateFactory.getStateAlias(),
                    stateFactory.getStateTimeout());
        }

        ChainedEvaluationFunction statemachineWithStateTimeoutsRaised = builder.build();

        for (int i = 0; i < 10; i++) { // do it 10 times, to make sure nothing is out of order
            // the first call is to set up the timers
            // prepare
            SimpleEvaluationContext context = new SimpleEvaluationContext(typedValues1);
            // execute
            statemachineWithStateTimeoutsRaised.eval(context); // second time, so that error can be passed

            // now the second call should trigger a state timeout
            // prepare
            SimpleEvaluationContext context2 = new SimpleEvaluationContext(null);
            // execute
            statemachineWithStateTimeoutsRaised.eval(context2); // second time, so that error can be passed
            // check
            Assert.assertNotNull(context2.getEvents());
            Assert.assertEquals(1, context2.getEvents().size());
            Assert.assertEquals(errorStateEvent, context2.getEvents().get(0));
        }
    }

    @Test
    public void evalWithOverallTimeoutRaised() { // -> a overall timeout should be raised
        // prepare a error extractor
        StateErrorExtractor errorOverallExtractor = (StateErrorExtractor) (events, ex, context) -> context.collect(
            errorOverallEvent);

        ChainedEvaluationFunction.Builder builder = ChainedEvaluationFunction.builder()
                .withErrorExtractor(errorOverallExtractor)
                .withOverallTimeoutMs(overallTimeout)
                .withStateCompleteExtractor(stateCompleteExtractor);
        for (StateConfig stateFactory : stateFactoriesWithOverallTimeoutsRaised) {
            builder.addEvaluationFunction(stateFactory.getFactory().create(), stateFactory.getStateAlias(),
                    stateFactory.getStateTimeout());
        }

        ChainedEvaluationFunction statemachineWithOverallTimeoutsRaised = builder.build();

        for (int i = 0; i < 10; i++) { // do it 10 times, to make sure nothing is out of order
            // the first call is to set up the timers
            // prepare
            SimpleEvaluationContext context = new SimpleEvaluationContext(typedValues1);
            // execute
            statemachineWithOverallTimeoutsRaised.eval(context);

            // now the second call should trigger a overall timeout
            // prepare
            SimpleEvaluationContext context2 = new SimpleEvaluationContext(null);
            // execute
            statemachineWithOverallTimeoutsRaised.eval(context2);
            // check
            Assert.assertNotNull(context2.getEvents());
            Assert.assertEquals(1, context2.getEvents().size());
            Assert.assertEquals(errorOverallEvent, context2.getEvents().get(0));
        }
    }

    @Test
    public void addEvaluationFunction() {
        // prepare a error extractor
        StateErrorExtractor errorOverallExtractor = (StateErrorExtractor) (events, ex, context) ->
            context.collect(errorEvent);

        // create a statemachine/chained EvaluationFunction
        ChainedEvaluationFunction function = ChainedEvaluationFunction.builder()
                .addEvaluationFunction(prototypeFunction, "alias")
                .addEvaluationFunction(prototypeFunction, "alias")
                .addEvaluationFunction(prototypeFunction, "alias")
            .withErrorExtractor(errorOverallExtractor)
            .withStateCompleteExtractor(stateCompleteExtractor)
            .build();

        for (int i = 0; i < 10; i++) { // do it 10 times, to make sure nothing is out of order

            SimpleEvaluationContext context = new SimpleEvaluationContext(typedValues1);
            for (int j = 0; j < 3; j++) {
//                context = new SimpleEvaluationContext(typedValues2);
                // execute
                function.eval(context);
            }
            // check
            Assert.assertNotNull(context.getEvents());
            Assert.assertEquals(1, context.getEvents().size());
            Assert.assertEquals(completeEvent, context.getEvents().get(0));
        }
    }


    @Test
    public void addEvaluationFunctionFactory() {
        // prepare a error extractor
        StateErrorExtractor errorOverallExtractor = (StateErrorExtractor) (events, ex, context) ->
            context.collect(errorEvent);

        // create a statemachine/chained EvaluationFunction
        CloneStateEvaluationFunctionFactory factory = CloneStateEvaluationFunctionFactory.builder()
            .withPrototype(prototypeFunction)
            .build();

        ChainedEvaluationFunction function = ChainedEvaluationFunction.builder()
            .addEvaluationFunctionFactory(factory, "alias")
            .addEvaluationFunctionFactory(factory, "alias")
            .addEvaluationFunctionFactory(factory, "alias")
            .withErrorExtractor(errorOverallExtractor)
            .withStateCompleteExtractor(stateCompleteExtractor)
            .build();

        for (int i = 0; i < 10; i++) { // do it 10 times, to make sure nothing is out of order

            SimpleEvaluationContext context = null;
            for (int j = 0; j < 3; j++) {
                // prepare
                context = new SimpleEvaluationContext(typedValues1);

                // execute
                function.eval(context);
            }
            // check
            Assert.assertNotNull(context.getEvents());
            Assert.assertEquals(1, context.getEvents().size());
            Assert.assertEquals(completeEvent, context.getEvents().get(0));
        }
    }
}
