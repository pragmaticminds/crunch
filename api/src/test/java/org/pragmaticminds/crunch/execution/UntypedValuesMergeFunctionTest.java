package org.pragmaticminds.crunch.execution;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.values.UntypedValues;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 03.10.2018
 */
public class UntypedValuesMergeFunctionTest implements Serializable {
    UntypedValuesMergeFunction mergeFunction;
    UntypedValues record1;
    UntypedValues record2;
    
    @Before
    public void setUp() throws Exception {
        mergeFunction = new UntypedValuesMergeFunction();
        Map<String, Object> values1 = new HashMap<>();
        values1.put("test1", 123L);
        Map<String, Object> values2 = new HashMap<>();
        values2.put("test2", "string");
        record1 = new UntypedValues("test", 123L, "prefix", values1);
        record2 = new UntypedValues("test", 124L, "prefix", values2);
    }
    
    @Test
    public void merge() {
        mergeFunction.merge(record1);
        UntypedValues result = (UntypedValues) mergeFunction.merge(record2);
        assertEquals(123L, (long)result.getValue("test1").getAsLong());
        assertEquals("string", result.getValue("test2").getAsString());
    }
    
    @Test
    public void mapWithoutState() {
        UntypedValues result = mergeFunction.mapWithoutState(record1, record2);
        assertEquals(123L, (long)result.getValue("test1").getAsLong());
        assertEquals("string", result.getValue("test2").getAsString());
    }
}