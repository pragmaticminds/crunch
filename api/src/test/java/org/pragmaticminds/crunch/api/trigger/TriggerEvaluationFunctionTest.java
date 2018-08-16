package org.pragmaticminds.crunch.api.trigger;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.pragmaticminds.crunch.api.pipe.SimpleEvaluationContext;
import org.pragmaticminds.crunch.api.trigger.extractor.EventExtractor;
import org.pragmaticminds.crunch.api.trigger.strategy.TriggerStrategy;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.Event;

import java.util.*;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.07.2018
 */
public class TriggerEvaluationFunctionTest {
    private long timestamp = 123L;
    private Event resultEvent = new Event(timestamp, "testEventName", "testSource");
    
    @Test
    public void processElementNotTriggered() {
        TriggerEvaluationFunction function = new TriggerEvaluationFunction.Builder()
            .withTriggerStrategy((TriggerStrategy) decisionBase -> false)
            .withEventExtractor((EventExtractor) (context) -> new ArrayList<>(Collections.singletonList(resultEvent)))
            .build();
        
        Map<String, Value> values = new HashMap<>();
        TypedValues typedValues = new TypedValues("testSource", timestamp, values);
        List<Event> resultEventList;
        try {
            SimpleEvaluationContext context = new SimpleEvaluationContext(typedValues);
            resultEventList = context.getEvents();
            function.eval(context);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        Assert.assertTrue(resultEventList.isEmpty());
    }
    
    @Test
    public void processElementTriggeredNoResults() {
        EventExtractor eventExtractor = Mockito.mock(EventExtractor.class);
        
        TriggerEvaluationFunction function = new TriggerEvaluationFunction.Builder()
            .withTriggerStrategy((TriggerStrategy) decisionBase -> true)
            .withEventExtractor(eventExtractor)
            .build();
        Map<String, Value> values = new HashMap<>();
        TypedValues typedValues = new TypedValues("testSource", timestamp, values);
        List<Event> resultEventList;
        try {
            SimpleEvaluationContext context = new SimpleEvaluationContext(typedValues);
            resultEventList = context.getEvents();
            function.eval(context);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        Mockito.verify(eventExtractor, Mockito.times(1)).process(Mockito.any());
        Assert.assertTrue(resultEventList.isEmpty());
    }
    
    @Test
    public void processElementOneResult() {
        Event resultEvent = new Event(timestamp, "testEventName", "testSource");
        TriggerEvaluationFunction function = new TriggerEvaluationFunction.Builder()
            .withTriggerStrategy((TriggerStrategy) decisionBase -> true)
            .withEventExtractor((EventExtractor) (context) -> new ArrayList<>(Collections.singletonList(resultEvent)))
            .build();
        Map<String, Value> values = new HashMap<>();
        TypedValues typedValues = new TypedValues("testSource", timestamp, values);
        List<Event> resultEventList;
        try {
            SimpleEvaluationContext context = new SimpleEvaluationContext(typedValues);
            resultEventList = context.getEvents();
            function.eval(context);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        Assert.assertEquals(1, resultEventList.size());
        Assert.assertTrue(resultEventList.contains(resultEvent));
    }
    
    @Test
    public void processElementManyResults() {
        TriggerEvaluationFunction function = new TriggerEvaluationFunction.Builder()
            .withTriggerStrategy((TriggerStrategy) decisionBase -> true)
            .withEventExtractor((EventExtractor) (context) ->
                new ArrayList<>(Arrays.asList(resultEvent, resultEvent, resultEvent))
            )
            .build();
        Map<String, Value> values = new HashMap<>();
        TypedValues typedValues = new TypedValues("testSource", timestamp, values);
        List<Event> resultEventList;
        try {
            SimpleEvaluationContext context = new SimpleEvaluationContext(typedValues);
            resultEventList = context.getEvents();
            function.eval(context);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        Assert.assertEquals(3, resultEventList.size());
        Assert.assertTrue(resultEventList.contains(resultEvent));
    }
    
    @Test
    public void processWithResultFilter() {
        TriggerEvaluationFunction function = new TriggerEvaluationFunction.Builder()
            .withTriggerStrategy((TriggerStrategy) decisionBase -> true)
            .withEventExtractor((EventExtractor) (context) ->
                new ArrayList<>(Arrays.asList(resultEvent, resultEvent, resultEvent))
            )
            .withFilter((event, values) -> values.getString("val").equals("string"))
            .build();
        Map<String, Value> values = new HashMap<>();
        values.put("val",Value.of("string"));
        TypedValues typedValues = new TypedValues("testSource", timestamp, values);
        List<Event> resultEventList;
        try {
            SimpleEvaluationContext context = new SimpleEvaluationContext(typedValues);
            resultEventList = context.getEvents();
            function.eval(context);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        Assert.assertEquals(3, resultEventList.size());
        Assert.assertTrue(resultEventList.contains(resultEvent));
    }
}