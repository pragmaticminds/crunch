package org.pragmaticminds.crunch.api.trigger.handler;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.SimpleEvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.extractor.Extractors;
import org.pragmaticminds.crunch.api.trigger.extractor.MapExtractor;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.GenericEvent;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.channel;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 20.09.2018
 */
public class ExtractorTriggerHandlerTest {
    
    private static final String EVENT_NAME = "TEST_EVENT";
    private MapExtractor extractor1;
    private MapExtractor extractor2;
    private SimpleEvaluationContext context;
    private String CHANNEL_1 = "test1";
    private String CHANNEL_2 = "test2";
    
    @Before
    public void setUp() throws Exception {
        extractor1 = Extractors.channelMapExtractor(channel(CHANNEL_1));
        extractor2 = Extractors.channelMapExtractor(channel(CHANNEL_2));
    
        Map<String, Value> values = new HashMap<>();
        values.put(CHANNEL_1, Value.of(1L));
        values.put(CHANNEL_2, Value.of(2L));
        
        MRecord record = TypedValues.builder()
            .source("test")
            .timestamp(123L)
            .values(values)
            .build();
        context = new SimpleEvaluationContext(record);
    }
    
    @Test
    public void withArrayConstructor() {
        ExtractorTriggerHandler handler = new GenericExtractorTriggerHandler(
            EVENT_NAME,
            extractor1,
            extractor2
        );
    
        executeAndCheckResults(handler);
    }
    
    @Test
    public void withListConstructor() {
        ExtractorTriggerHandler handler = new GenericExtractorTriggerHandler(
            EVENT_NAME,
            Arrays.asList(extractor1,extractor2)
        );
        
        executeAndCheckResults(handler);
    }
    
    private void executeAndCheckResults(ExtractorTriggerHandler handler) {
        handler.handle(context);
        List<GenericEvent> events = context.getEvents();
        assertEquals(1, events.size());
        GenericEvent event = events.get(0);
        assertEquals(1L, (long)event.getParameter(CHANNEL_1).getAsLong());
        assertEquals(2L, (long)event.getParameter(CHANNEL_2).getAsLong());
    }
}