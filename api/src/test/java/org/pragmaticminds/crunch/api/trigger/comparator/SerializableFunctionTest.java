package org.pragmaticminds.crunch.api.trigger.comparator;

import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;

import static org.junit.Assert.*;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 08.10.2018
 */
public class SerializableFunctionTest {
    @Test
    public void serialization() {
        SerializableFunction<String, String> function = s -> s;
        SerializableFunction<String, String> clone = ClonerUtil.clone(function);
        assertEquals("string", clone.apply("string"));
    }
}