package org.pragmaticminds.crunch.api.trigger.comparator;

import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;

import static org.junit.Assert.*;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 08.10.2018
 */
public class SerializableResultFunctionTest {
    @Test
    public void serialization() {
        SerializableResultFunction<String> function = () -> "string";
        SerializableResultFunction<String> clone = ClonerUtil.clone(function);
        assertEquals("string", clone.get());
    }
}