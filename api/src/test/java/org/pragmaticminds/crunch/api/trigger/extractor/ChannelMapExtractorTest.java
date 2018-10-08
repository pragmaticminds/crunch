package org.pragmaticminds.crunch.api.trigger.extractor;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;
import org.pragmaticminds.crunch.api.pipe.SimpleEvaluationContext;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.channel;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 20.09.2018
 */
public class ChannelMapExtractorTest {
    private Supplier supplier1;
    private Supplier supplier2;
    private Supplier supplier3;
    
    private MRecord record;
    
    @Before
    public void setUp() throws Exception {
        supplier1 = channel("test1");
        supplier2 = channel("test2");
        supplier3 = channel("test3");
        
        Map<String, Object> values = new HashMap<>();
        values.put("test1", 1L);
        values.put("test2", 2L);
        values.put("test3", 3L);
        
        record = UntypedValues.builder()
            .source("test")
            .timestamp(123L)
            .prefix("")
            .values(values)
            .build();
    }
    
    @Test
    public void withArrayConstructor() {
        SimpleEvaluationContext context1 = new SimpleEvaluationContext(record);
        SimpleEvaluationContext context2 = new SimpleEvaluationContext(record);
        
        ChannelMapExtractor extractor = new ChannelMapExtractor(supplier1, supplier2, supplier3);
        ChannelMapExtractor clone = ClonerUtil.clone(extractor);
    
        executeAndCheckResults("test", context1, extractor);
    
        executeAndCheckResults("test", context2, clone);
    }
    
    @Test
    public void withListConstructor() {
        SimpleEvaluationContext context1 = new SimpleEvaluationContext(record);
        SimpleEvaluationContext context2 = new SimpleEvaluationContext(record);
    
        ChannelMapExtractor extractor = new ChannelMapExtractor(Arrays.asList(supplier1, supplier2, supplier3));
        ChannelMapExtractor clone = ClonerUtil.clone(extractor);
    
        executeAndCheckResults("test", context1, extractor);
        executeAndCheckResults("test", context2, clone);
    }
    
    @Test
    public void withMapConstructor() {
        SimpleEvaluationContext context1 = new SimpleEvaluationContext(record);
        SimpleEvaluationContext context2 = new SimpleEvaluationContext(record);
    
        Map<Supplier, String> map = new HashMap<>();
        map.put(supplier1, "t1");
        map.put(supplier2, "t2");
        map.put(supplier3, "t3");
        ChannelMapExtractor extractor = new ChannelMapExtractor(map);
        ChannelMapExtractor clone = ClonerUtil.clone(extractor);
    
        executeAndCheckResults("t", context1, extractor);
        executeAndCheckResults("t", context2, clone);
    }
    
    private void executeAndCheckResults(String prefix, SimpleEvaluationContext context, ChannelMapExtractor extractor) {
        Map<String, Value> result = extractor.extract(context);
        assertEquals(3, result.size());
        assertEquals(1L, (long)result.get(String.format("%s1", prefix)).getAsLong());
        assertEquals(2L, (long)result.get(String.format("%s2", prefix)).getAsLong());
        assertEquals(3L, (long)result.get(String.format("%s3", prefix)).getAsLong());
    }
}