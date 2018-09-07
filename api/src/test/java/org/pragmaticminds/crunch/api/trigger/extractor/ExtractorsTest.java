package org.pragmaticminds.crunch.api.trigger.extractor;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.SimpleEvaluationContext;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.Event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 14.08.2018
 */
public class ExtractorsTest {
    private EvaluationContext evaluationContext;
    
    @Before
    public void setUp() throws Exception {
        // create a context with three values in the TypedValues object
        Map<String, Value> valueMap = new HashMap<>();
        valueMap.put("channel1", Value.of(1L));
        valueMap.put("channel2", Value.of(2L));
        valueMap.put("channel3", Value.of(3L));
        TypedValues typedValues = TypedValues.builder()
            .timestamp(System.currentTimeMillis())
            .source("test")
            .values(valueMap)
            .build();
        evaluationContext = new SimpleEvaluationContext(typedValues);
    }
    
    /** Extract 3 channels from a {@link TypedValues} */
    @Test
    public void valuesExtractor() {
        EventExtractor eventExtractor = Extractors.valuesExtractor(
            "channel1",
            "channel2",
            "channel3",
            "null" // this will be ignored in the results
        );
        ArrayList<Event> events = new ArrayList<>(eventExtractor.process(evaluationContext));
        assertEquals(1, events.size());
        assertEquals(1L, (long) events.get(0).getParameter("channel1").getAsLong());
        assertEquals(2L, (long) events.get(0).getParameter("channel2").getAsLong());
        assertEquals(3L, (long) events.get(0).getParameter("channel3").getAsLong());
    }
    
    /** Extract 3 channels from a {@link TypedValues} and rename them with their alias */
    @Test
    public void valuesExtractorWithAliasMapping() {
        Map<String, String> aliasedChannels = new HashMap<>();
        aliasedChannels.put("channel1", "key1");
        aliasedChannels.put("channel2", "key2");
        aliasedChannels.put("channel3", "key3");
        aliasedChannels.put("null", "null"); // this will be ignored for the results
        EventExtractor eventExtractor = Extractors.valuesExtractor(aliasedChannels);
        ArrayList<Event> events = new ArrayList<>(eventExtractor.process(evaluationContext));
        assertEquals(1, events.size());
        assertEquals(1L, (long) events.get(0).getParameter("key1").getAsLong());
        assertEquals(2L, (long) events.get(0).getParameter("key2").getAsLong());
        assertEquals(3L, (long) events.get(0).getParameter("key3").getAsLong());
    }
}