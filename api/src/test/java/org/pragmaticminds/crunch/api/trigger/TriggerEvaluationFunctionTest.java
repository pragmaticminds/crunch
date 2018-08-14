package org.pragmaticminds.crunch.api.trigger;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.07.2018
 */
// FIXME do test without flink stuff.
public class TriggerEvaluationFunctionTest {
//    private Event resultEvent = new Event(123l, "testEventName", "testSource");
//
//    @Test
//    public void processElementNotTriggered() {
//        TriggerEvaluationFunction<Integer> function = new TriggerEvaluationFunction.Builder<Integer>()
//            .withSupplier((ValueSupplier<Integer>) values -> 0)
//            .withTriggerStrategy((TriggerStrategy<Integer>) decisionBase -> false)
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
//        TriggerEvaluationFunction<Boolean> function = new TriggerEvaluationFunction.Builder<Boolean>()
//            .withSupplier((ValueSupplier<Boolean>) values -> true)
//            .withTriggerStrategy((TriggerStrategy<Boolean>) decisionBase -> true)
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
//        TriggerEvaluationFunction<Boolean> function = new TriggerEvaluationFunction.Builder<Boolean>()
//            .withSupplier((ValueSupplier<Boolean>) values -> true)
//            .withTriggerStrategy((TriggerStrategy<Boolean>) decisionBase -> true)
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
//        TriggerEvaluationFunction<Boolean> function = new TriggerEvaluationFunction.Builder<Boolean>()
//            .withSupplier((ValueSupplier<Boolean>) values -> true)
//            .withTriggerStrategy((TriggerStrategy<Boolean>) decisionBase -> true)
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