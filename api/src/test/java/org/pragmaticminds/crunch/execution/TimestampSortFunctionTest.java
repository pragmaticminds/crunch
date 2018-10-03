package org.pragmaticminds.crunch.execution;

import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 03.10.2018
 */
public class TimestampSortFunctionTest implements Serializable {
    
    private TimestampSortFunction<String> sortFunction;
    
    @Before
    public void setUp() throws Exception {
        sortFunction = new TimestampSortFunction<String>(50);
    }
    
    @Test
    public void process() {
        sortFunction.process(1L, 2L, "test1");
    }
    
    @Test
    public void onTimer() {
        sortFunction.onTimer(2L);
    }
    
    @Test
    public void sorting() {
        TimestampSortFunction<String> function = new TimestampSortFunction<>();
        function.process(60L, 10L, "test5");
        function.process(0L, 10L, "test6"); // should be discarded
        function.process(25L, 10L, "test3");
        function.process(55L, 10L, "test4");
        function.process(15L, 10L, "test1");
        function.process(20L, 10L, "test2");
    
        // should ignore "test4" and "test5"
        Collection<String> strings = function.onTimer(50L);
        
        assertEquals(3, strings.size());
        assertEquals(Arrays.asList("test1", "test2", "test3"), strings);
    
    }
}