package org.pragmaticminds.crunch.api.trigger.filter;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.util.HashMap;
import java.util.Map;

import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.stringChannel;
import static org.pragmaticminds.crunch.api.trigger.filter.EventFilters.onValueChanged;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 14.08.2018
 */
public class EventFiltersTest {
    
    private TypedValues values1;
    private TypedValues values2;
    private TypedValues valuesNull;
    
    @Before
    public void setUp() throws Exception {
        Map<String, Value> valueMap1 = new HashMap<>();
        valueMap1.put("val", Value.of("string1"));
        values1 = TypedValues.builder().source("test").timestamp(System.currentTimeMillis()).values(valueMap1).build();
    
        Map<String, Value> valueMap2 = new HashMap<>();
        valueMap2.put("val", Value.of("string2"));
        values2 = TypedValues.builder().source("test").timestamp(System.currentTimeMillis()).values(valueMap2).build();
    
        Map<String, Value> valueMap3 = new HashMap<>();
        valuesNull = TypedValues.builder().source("test").timestamp(System.currentTimeMillis()).values(valueMap3).build();
    }
    
    @Test
    public void valueChangedTest() {
        // value test
        EventFilter filter = onValueChanged(stringChannel("val"));
    
        // first value receive -> false
        Assert.assertFalse(filter.apply(null, values1));
        // first change -> true
        Assert.assertTrue(filter.apply(null, values2));
        // value is null -> false
        Assert.assertFalse(filter.apply(null, valuesNull));
        // value change -> true
        Assert.assertTrue(filter.apply(null, values1));
        // no change -> false
        Assert.assertFalse(filter.apply(null, values1));
        // value is null again -> false
        Assert.assertFalse(filter.apply(null, valuesNull));
    
    }
}