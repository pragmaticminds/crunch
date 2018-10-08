package org.pragmaticminds.crunch.api.trigger.comparator;

import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;

import static org.junit.Assert.assertEquals;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 08.10.2018
 */
public class SerializableActionTest {
    @Test
    public void serializationTest() {
        SerializableAction<String> action = s -> assertEquals("string", s);
        action.accept("string");
        SerializableAction<String> clone = ClonerUtil.clone(action);
        clone.accept("string");
    }
}