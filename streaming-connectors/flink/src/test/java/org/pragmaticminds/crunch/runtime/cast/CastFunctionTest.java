package org.pragmaticminds.crunch.runtime.cast;

import org.junit.Test;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.UntypedValues;

import java.sql.Date;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author kerstin
 * Created by kerstin on 03.11.17.
 */
public class CastFunctionTest {

    private CastFunction castFunction = new CastFunction();

    @Test
    public void map() {
        long timestamp = Instant.now().toEpochMilli();
        Map<String, Object> map = new HashMap<>();
        map.put("0", Boolean.FALSE);
        map.put("1", Date.from(Instant.ofEpochMilli(timestamp)));
        map.put("2", 2.0);
        map.put("3", 3L);
        map.put("4", "4");

        UntypedValues untypedValues = new UntypedValues("source", timestamp, "testPlc", map);
        TypedValues typedValues = castFunction.map(untypedValues);

        assertNotNull(typedValues);
        assertEquals(untypedValues.getSource(), typedValues.getSource());
        assertEquals(untypedValues.getTimestamp(), typedValues.getTimestamp());
    }

}