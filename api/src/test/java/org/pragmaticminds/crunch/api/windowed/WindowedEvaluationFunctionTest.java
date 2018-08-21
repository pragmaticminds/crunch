package org.pragmaticminds.crunch.api.windowed;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.SimpleEvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.api.windowed.extractor.WindowExtractor;
import org.pragmaticminds.crunch.events.Event;

import java.io.Serializable;
import java.util.*;

import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.booleanChannel;
import static org.pragmaticminds.crunch.api.windowed.Windows.bitActive;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public class WindowedEvaluationFunctionTest implements Serializable {
    
    private WindowedEvaluationFunction function;
    private MRecord record1;
    private MRecord record2;
    private MRecord record3;
    private MRecord record4;
    private MRecord record5;
    
    @Before
    public void setUp() throws Exception {
        function = WindowedEvaluationFunction.builder()
            // set window
            .recordWindow(bitActive(booleanChannel("flag")))
            // set extractor
            .extractor(new MaxWindowExtractor())
            // set filter
            .filter((event, records) -> true)
            .build();
    
        // create test processing data
        TypedValues.TypedValuesBuilder typedValuesBuilder = TypedValues.builder()
            .source("test")
            .timestamp(System.currentTimeMillis());
    
        Map<String, Value> valueMap1 = new HashMap<>();
        valueMap1.put("flag", Value.of(false));
        record1 = typedValuesBuilder.values(valueMap1).build();
    
        Map<String, Value> valueMap2 = new HashMap<>();
        valueMap2.put("flag", Value.of(true));
        valueMap2.put("value", Value.of(1.0));
        record2 = typedValuesBuilder.values(valueMap2).build();
    
        Map<String, Value> valueMap3 = new HashMap<>();
        valueMap3.put("flag", Value.of(true));
        valueMap3.put("value", Value.of(2.0));
        record3 = typedValuesBuilder.values(valueMap3).build();
    
        Map<String, Value> valueMap4 = new HashMap<>();
        valueMap4.put("flag", Value.of(true));
        valueMap4.put("value", Value.of(3.0));
        record4 = typedValuesBuilder.values(valueMap4).build();
    
        Map<String, Value> valueMap5 = new HashMap<>();
        valueMap5.put("flag", Value.of(false));
        record5 = typedValuesBuilder.values(valueMap5).build();
    }
    
    /**
     * Simulates a windowed processing situation
     * Messages:
     *  1. window is closed
     *  2. - 4. window is open
     *  1. window is closed -> on the message processing is expected to run
     */
    @Test
    public void eval() {
        SimpleEvaluationContext context = new SimpleEvaluationContext(record1);
        function.eval(context);
        Assert.assertEquals(0, context.getEvents().size());
    
        context = new SimpleEvaluationContext(record2);
        function.eval(context);
        Assert.assertEquals(0, context.getEvents().size());
    
        context = new SimpleEvaluationContext(record3);
        function.eval(context);
        Assert.assertEquals(0, context.getEvents().size());
    
        context = new SimpleEvaluationContext(record4);
        function.eval(context);
        Assert.assertEquals(0, context.getEvents().size());
    
        context = new SimpleEvaluationContext(record5);
        function.eval(context);
        List<Event> events = context.getEvents();
        Assert.assertEquals(1, events.size());
        Assert.assertEquals(3.0, events.get(0).getParameter("maxValue").getAsDouble(), 0.00001);
        
    }
    
    private class MaxWindowExtractor implements WindowExtractor {
        double maxValue = 0.0;
        
        @Override
        public void apply(MRecord record) {
            double value = record.getDouble("value");
            if (value > maxValue) {
                maxValue = value;
            }
        }
        
        @Override
        public void finish(EvaluationContext context) {
            // create a result event
            context.collect(
                context.getEventBuilder()
                    .withEvent("maxValue")
                    .withTimestamp(context.get().getTimestamp())
                    .withSource("test")
                    .withParameter("maxValue", maxValue)
                    .build()
            );
        }
    }
}