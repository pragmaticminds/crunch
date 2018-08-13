package org.pragmaticminds.crunch.api.values;

import org.junit.Test;
import org.pragmaticminds.crunch.api.values.dates.LongValue;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the UntypedValues.
 *
 * @author julian
 * Created by julian on 03.11.17
 */
public class UntypedValuesTest {

    @Test
    public void getTimestamp_fromInterface_worksAsExpected() {
        ValueEvent event = UntypedValues.builder().timestamp(13L).build();

        assertEquals(13L, event.getTimestamp());
    }

    @Test
    public void toTypedValues() {
        UntypedValues untypedValues = UntypedValues.builder()
                .timestamp(13L)
                .prefix("")
                .source("test")
                .values(Collections.singletonMap("key", 42L))
                .build();

        TypedValues typedValues = untypedValues.toTypedValues();

        assertEquals(untypedValues.getTimestamp(), typedValues.getTimestamp());
        assertEquals(untypedValues.getSource(), typedValues.getSource());
        assertTrue(LongValue.class.isAssignableFrom(typedValues.getValues().get("key").getClass()));
        assertEquals(42L, untypedValues.getValues().get("key"));
    }

    @Test(expected = ClassCastException.class)
    public void toTypeValues_fails() {
        UntypedValues untypedValues = UntypedValues.builder()
                .timestamp(13L)
                .source("test")
                .prefix("")
                .values(Collections.singletonMap("key", Instant.now()))
                .build();

        untypedValues.toTypedValues();
    }

    @Test
    public void filterChannelNames_keepOne() {
        UntypedValues untypedValues = getUntypedValues();

        untypedValues = untypedValues.filterChannels(Collections.singleton("channel1"));

        assertEquals(1, untypedValues.getValues().size());
    }

    @Test
    public void filterChannelNames_keepAll() {
        UntypedValues untypedValues = getUntypedValues();

        HashSet<String> names = new HashSet<>();
        names.add("channel1");
        names.add("channel2");
        names.add("channel3");

        untypedValues = untypedValues.filterChannels(names);

        assertEquals(3, untypedValues.getValues().size());
    }

    @Test
    public void filterChannelNames_keepNone_isEmpty() {
        UntypedValues untypedValues = getUntypedValues();

        HashSet<String> names = new HashSet<>();
        names.add("channel4");
        names.add("channel5");
        names.add("channel6");

        untypedValues = untypedValues.filterChannels(names);

        assertEquals(0, untypedValues.getValues().size());
        assertTrue(untypedValues.isEmpty());
    }

    private UntypedValues getUntypedValues() {
        HashMap<String, Object> values = new HashMap<>();
        values.put("channel1", 1);
        values.put("channel2", 2);
        values.put("channel3", 3);
        return UntypedValues.builder()
                .timestamp(13L)
                .source("test")
                .prefix("")
                .values(values)
                .build();
    }
}