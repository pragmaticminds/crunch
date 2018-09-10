package org.pragmaticminds.crunch.api.state;

import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.pipe.SimpleEvaluationContext;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.events.Event;
import org.pragmaticminds.crunch.events.EventBuilder;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.*;

/** *
 * @author Erwin Wagasow
 * @author kerstin
 * Created by Erwin Wagasow on 09.08.2018
 */
public class MultiStepEvaluationFunctionTest implements Serializable {

    private ErrorExtractor errorExtractor = (ErrorExtractor) (events, ex, context) -> context.collect(new Event(0L, ex.getClass().getName(), ""));
    private EvaluationCompleteExtractor evaluationCompleteExtractor = (EvaluationCompleteExtractor) (events, context) -> {
        for (Map.Entry<String, Event> stringEventEntry : events.entrySet()) {
            context.collect(stringEventEntry.getValue());
        }
    };
    private Event successEvent = EventBuilder.anEvent()
            .withTimestamp(System.currentTimeMillis())
            .withSource("test")
            .withEvent("success")
            .withParameter("string", "string")
            .build();
    private TypedValues typedValues = TypedValues.builder().source("test").values(Collections.emptyMap()).build();

    @Test
    public void evalDefaultTimeOut_noTimeOutOccurs() { // -> processing should be successful with no timers set
        // create instance of the MultiStepEvaluationFunction with parameters for this test
        MultiStepEvaluationFunction stateMachine = MultiStepEvaluationFunction.builder()
            .addEvaluationFunction((EvaluationFunction) ctx -> ctx.collect(successEvent), "success1", 10)
            .addEvaluationFunction((EvaluationFunction) ctx -> ctx.collect(successEvent), "success2", 10)
            .withEvaluationCompleteExtractor(evaluationCompleteExtractor)
            .withErrorExtractor(errorExtractor)
            .build();
    
        SimpleEvaluationContext context;
        context = new SimpleEvaluationContext(typedValues);
        
        stateMachine.eval(context);
    
        context = new SimpleEvaluationContext(
            new TypedValues(
                "",
                typedValues.getTimestamp() + 9,
                typedValues.getValues()
            )
        );
        stateMachine.eval(context);
        
        assertNotNull(context.getEvents());
        assertEquals(2, context.getEvents().size());
        for (Event event : context.getEvents()) {
            assertEquals(successEvent, event);
        }
    }

    @Test
    public void eval_withStepTimeout() { // -> a state timeout should be raised
        MultiStepEvaluationFunction stateMachine = MultiStepEvaluationFunction.builder()
                .withErrorExtractor(errorExtractor)
                .withEvaluationCompleteExtractor(evaluationCompleteExtractor)
                .addEvaluationFunction((EvaluationFunction) ctx -> ctx.collect(successEvent), "success", 10)
                .addEvaluationFunction((EvaluationFunction) ctx -> { }, "timeout")
                .build();

            SimpleEvaluationContext context = new SimpleEvaluationContext(typedValues);
            stateMachine.eval(context); // second time, so that error can be passed
            // now the second call should trigger a state timeout because it's timestamp is 100 but can be 10 at maximum
            SimpleEvaluationContext context2 = new SimpleEvaluationContext(new TypedValues("", typedValues.getTimestamp()+100, typedValues.getValues()));
            stateMachine.eval(context2);
            assertTrue(context.getEvents().isEmpty());
            assertEquals(1, context2.getEvents().size());
            assertEquals(StepTimeoutException.class.getName(), context2.getEvents().get(0).getEventName());
    }

    @Test
    public void eval_withOverallTimeout() { // -> a overall timeout should be raised
        MultiStepEvaluationFunction stateMachine = MultiStepEvaluationFunction.builder()
                .withErrorExtractor(errorExtractor)
                .withOverallTimeoutMs(10)
                .withEvaluationCompleteExtractor(evaluationCompleteExtractor)
                .addEvaluationFunction((EvaluationFunction) ctx -> ctx.collect(successEvent), "success1")
                .addEvaluationFunction((EvaluationFunction) ctx -> { }, "timeout1")
                .addEvaluationFunction((EvaluationFunction) ctx -> { }, "timeout2")
                .build();
        SimpleEvaluationContext context = new SimpleEvaluationContext(typedValues);
        stateMachine.eval(context);
        SimpleEvaluationContext context2 = new SimpleEvaluationContext(new TypedValues("", typedValues.getTimestamp()+5, typedValues.getValues()));
        stateMachine.eval(context2);
        SimpleEvaluationContext context3 = new SimpleEvaluationContext(new TypedValues("", typedValues.getTimestamp()+11, typedValues.getValues()));
        stateMachine.eval(context3);
        assertTrue(context.getEvents().isEmpty());
        assertTrue(context2.getEvents().isEmpty());
            assertEquals(1, context3.getEvents().size());
        assertEquals(OverallTimeoutException.class.getName(), context3.getEvents().get(0).getEventName());
    }

    @Test
    public void test_builder() {
        MultiStepEvaluationFunction.Builder builder = MultiStepEvaluationFunction.builder()
                .withErrorExtractor((ErrorExtractor) (events, ex, context) -> { })
                .withEvaluationCompleteExtractor((EvaluationCompleteExtractor) (events, context) -> { })
                .withOverallTimeoutMs(0L)
                .addEvaluationFunction((EvaluationFunction) ctx -> {}, "")
                .addEvaluationFunction((EvaluationFunction) ctx -> {}, "", 0L)
                .addEvaluationFunctionFactory((EvaluationFunctionStateFactory) () -> null, "")
                .addEvaluationFunctionFactory((EvaluationFunctionStateFactory) () -> null, "", 0L);

        builder.build();
    }

}
