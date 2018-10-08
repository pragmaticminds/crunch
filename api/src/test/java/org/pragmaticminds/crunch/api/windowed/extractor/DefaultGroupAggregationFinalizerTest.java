package org.pragmaticminds.crunch.api.windowed.extractor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;
import org.pragmaticminds.crunch.api.pipe.SimpleEvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.events.GenericEvent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 23.08.2018
 */
public class DefaultGroupAggregationFinalizerTest {
    
    private DefaultGenericEventGroupAggregationFinalizer finalizer;
    private DefaultGenericEventGroupAggregationFinalizer clone;
    private Map<String, Object>              aggregatedValues;
    private SimpleEvaluationContext          context;
    
    @Before
    public void setUp() throws Exception {
        
        // create the finalizer
        finalizer = new DefaultGenericEventGroupAggregationFinalizer();
        clone = ClonerUtil.clone(finalizer);
        
        // create the aggregated values
        aggregatedValues = new HashMap<>();
        aggregatedValues.put("string", "string1");
        aggregatedValues.put("double", 23D);
    
        // create values for the record
        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put("x", "test");
        
        // create a MRecord
        MRecord values = UntypedValues.builder()
            .values(valueMap)
            .timestamp(System.currentTimeMillis())
            .prefix("")
            .source("test")
            .build();
        
        // create context
        context = new SimpleEvaluationContext(values);
    }
    
    @Test
    public void onFinalize() {
        // call method
        test(finalizer);
    }
    
    @Test
    public void onFinalizeWithClone() {
        // call method
        test(clone);
    }
    
    private void test(DefaultGenericEventGroupAggregationFinalizer clone) {
        clone.onFinalize(aggregatedValues, context);
        List<GenericEvent> events = context.getEvents();
        
        // check amount of events
        Assert.assertEquals(1, events.size());
        GenericEvent event = events.get(0);
        
        // check the group naming is set right
        Assert.assertTrue(event.getEventName().startsWith("GROUP_"));
        
        // check the parameters are set right
        Assert.assertEquals("string1", event.getParameter("string").getAsString());
        Assert.assertEquals(23D, event.getParameter("double").getAsDouble(), 0.0001);
    }
}