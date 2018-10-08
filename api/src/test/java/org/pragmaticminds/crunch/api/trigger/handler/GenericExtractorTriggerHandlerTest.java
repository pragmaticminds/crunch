package org.pragmaticminds.crunch.api.trigger.handler;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.SimpleEvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.extractor.MapExtractor;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.GenericEvent;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 01.10.2018
 */
public class GenericExtractorTriggerHandlerTest {
    
    private GenericExtractorTriggerHandler handler;
    private EvaluationContext<GenericEvent> context;
    
    @Before
    public void setUp() throws Exception {
        handler = new GenericExtractorTriggerHandler("test", new InnerMapExtractor());
    
        Map<String, Object> values = new HashMap<>();
        values.put("test", 123L);
        MRecord record = UntypedValues.builder()
            .source("test")
            .prefix("")
            .timestamp(System.currentTimeMillis())
            .values(values)
            .build();
        context = new SimpleEvaluationContext<>(record);
    }
    
    @Test
    public void createEvent() {
        Map<String, Value> parameters = new HashMap<>();
        parameters.put("test", Value.of(123L));
        GenericEvent event = handler.createEvent("testEvent", context, parameters);
        
        assertEquals(123L,(long)event.getParameter("test").getAsLong());
    
        // serializable test
        GenericExtractorTriggerHandler clone = ClonerUtil.clone(handler);
        event = clone.createEvent("testEvent", context, parameters);
        assertEquals(123L,(long)event.getParameter("test").getAsLong());
    }
    
    private static class InnerMapExtractor implements MapExtractor {
        @Override
        public Map<String, Value> extract(EvaluationContext context) {
            Map<String, Value> map = new HashMap<>();
            map.put("test", context.get().getValue("test"));
            return map;
        }
    }
}