package org.pragmaticminds.crunch.api.trigger.extractor;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.SimpleEvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 20.09.2018
 */
public class AllChannelMapExtractorTest {
    private AllChannelMapExtractor extractor;
    private EvaluationContext context;
    
    @Before
    public void setUp() throws Exception {
        Map<String, Value> values = new HashMap<>();
        values.put("test1", Value.of(1L));
        values.put("test2", Value.of(2L));
        values.put("test3", Value.of(3L));
        
        MRecord record = new TypedValues(
            "source",
            123L,
            values
        );
        context = new SimpleEvaluationContext(record);
        
        extractor = new AllChannelMapExtractor();
    }
    
    @Test
    public void extract() {
        Map<String, Value> results = extractor.extract(context);
        
        assertEquals(3, results.size());
        assertEquals(1L, (long)results.get("test1").getAsLong());
        assertEquals(2L, (long)results.get("test2").getAsLong());
        assertEquals(3L, (long)results.get("test3").getAsLong());
    }
}