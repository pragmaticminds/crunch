package org.pragmaticminds.crunch.api.trigger;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.trigger.extractor.EventExtractor;
import org.pragmaticminds.crunch.api.trigger.strategy.TriggerStrategy;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.Event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * FIXME Implement this without Dependency on Flink
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.07.2018
 */
public class TriggerEvaluationFunctionTest {

    private Event resultEvent = new Event(123l, "testEventName", "testSource");
    
//    @Test
//    public void processElementNotTriggered() {
//        TriggerEvaluationFunction function = new TriggerEvaluationFunction.Builder()
//            .withTriggerStrategy((TriggerStrategy) decisionBase -> false)
//            .withEventExtractor((EventExtractor) (context) -> context.collect(resultEvent))
//            .build();
//
//        Map<String, Value> values = new HashMap<>();
//        TypedValues typedValues = new TypedValues("testSource", 123l, values);
//        List<Event> resultEventList = new ArrayList<>();
//        ListCollector<Event> out = new ListCollector<>(resultEventList);
//
//        try {
//            EvaluationContext context = CollectorEvaluationContext.builder().withValue(typedValues).withOut(out).build();
//            function.eval(context);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        Assert.assertTrue(resultEventList.isEmpty());
//    }
//
//    @Test
//    public void processElementTriggeredNoResults() {
//        EventExtractor eventExtractor = Mockito.mock(EventExtractor.class);
//
//        TriggerEvaluationFunction function = new TriggerEvaluationFunction.Builder()
//            .withTriggerStrategy((TriggerStrategy) decisionBase -> true)
//            .withEventExtractor(eventExtractor)
//            .build();
//        Map<String, Value> values = new HashMap<>();
//        TypedValues typedValues = new TypedValues("testSource", 123l, values);
//        List<Event> resultEventList = new ArrayList<>();
//        ListCollector<Event> out = new ListCollector<>(resultEventList);
//        try {
//            EvaluationContext context = CollectorEvaluationContext.builder().withValue(typedValues).withOut(out).build();
//            function.eval(context);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        Mockito.verify(eventExtractor, Mockito.times(1)).process(Mockito.any());
//        Assert.assertTrue(resultEventList.isEmpty());
//    }
//
//    @Test
//    public void processElementOneResult() {
//        Event resultEvent = new Event(123l, "testEventName", "testSource");
//        TriggerEvaluationFunction function = new TriggerEvaluationFunction.Builder()
//            .withTriggerStrategy((TriggerStrategy) decisionBase -> true)
//            .withEventExtractor((EventExtractor) (context) -> context.collect(resultEvent))
//            .build();
//        Map<String, Value> values = new HashMap<>();
//        TypedValues typedValues = new TypedValues("testSource", 123l, values);
//        List<Event> resultEventList = new ArrayList<>();
//        ListCollector<Event> out = new ListCollector<>(resultEventList);
//        try {
//            EvaluationContext context = CollectorEvaluationContext.builder().withValue(typedValues).withOut(out).build();
//            function.eval(context);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        Assert.assertEquals(1, resultEventList.size());
//        Assert.assertTrue(resultEventList.contains(resultEvent));
//    }
//
//    @Test
//    public void processElementManyResults() {
//        TriggerEvaluationFunction function = new TriggerEvaluationFunction.Builder()
//            .withTriggerStrategy((TriggerStrategy) decisionBase -> true)
//            .withEventExtractor((EventExtractor) (context) -> {
//                context.collect(resultEvent);
//                context.collect(resultEvent);
//                context.collect(resultEvent);
//            })
//            .build();
//        Map<String, Value> values = new HashMap<>();
//        TypedValues typedValues = new TypedValues("testSource", 123l, values);
//        List<Event> resultEventList = new ArrayList<>();
//        ListCollector<Event> out = new ListCollector<>(resultEventList);
//        try {
//            EvaluationContext context = CollectorEvaluationContext.builder().withValue(typedValues).withOut(out).build();
//            function.eval(context);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        Assert.assertEquals(3, resultEventList.size());
//        Assert.assertTrue(resultEventList.contains(resultEvent));
//    }
}