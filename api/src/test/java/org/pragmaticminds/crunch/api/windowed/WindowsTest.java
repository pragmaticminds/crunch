package org.pragmaticminds.crunch.api.windowed;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.trigger.comparator.NamedSupplier;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.util.HashMap;
import java.util.Map;

import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.booleanChannel;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public class WindowsTest {
    
    TypedValues valuesTrue;
    TypedValues valuesFalse;
    
    @Before
    public void setUp() throws Exception {
        TypedValues.TypedValuesBuilder valuesBuilder = TypedValues.builder()
            .timestamp(System.currentTimeMillis())
            .source("test");
    
        Map<String, Value> valueMap1 = new HashMap<>();
        valueMap1.put("flag", Value.of(true));
        valuesTrue = valuesBuilder.values(valueMap1).build();
        
        Map<String, Value> valueMap2 = new HashMap<>();
        valueMap2.put("flag", Value.of(false));
        valuesFalse = valuesBuilder.values(valueMap2).build();
    }
    
    @Test
    public void testBitActive() {
        RecordWindow recordWindow = Windows.bitActive(new NamedSupplier<>("flag", values -> values.getBoolean("flag")));
        
        boolean result = recordWindow.inWindow(valuesTrue);
        Assert.assertTrue(result);
        
        result = recordWindow.inWindow(valuesFalse);
        Assert.assertFalse(result);
    }
    
    @Test
    public void testBitNotActive() throws Exception {
        RecordWindow recordWindow = Windows.bitNotActive(booleanChannel("flag"));
    
        boolean result = recordWindow.inWindow(valuesTrue);
        Assert.assertFalse(result);
    
        result = recordWindow.inWindow(valuesFalse);
        Assert.assertTrue(result);
    }
    
    @Test
    public void testValueEquals() throws Exception {
        RecordWindow recordWindow = Windows.valueEquals(booleanChannel("flag"), true);
    
        boolean result = recordWindow.inWindow(valuesTrue);
        Assert.assertTrue(result);
    
        result = recordWindow.inWindow(valuesFalse);
        Assert.assertFalse(result);
    }
    
    @Test
    public void testValueNotEquals() throws Exception {
        RecordWindow recordWindow = Windows.valueNotEquals(booleanChannel("flag"), true);
    
        boolean result = recordWindow.inWindow(valuesTrue);
        Assert.assertFalse(result);
    
        result = recordWindow.inWindow(valuesFalse);
        Assert.assertTrue(result);
    }
}
