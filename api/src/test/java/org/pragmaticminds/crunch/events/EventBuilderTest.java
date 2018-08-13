package org.pragmaticminds.crunch.events;

import org.junit.Test;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.sql.Date;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

/**
 * Tests the functionality of the EventBuilder
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 21.11.2017
 */
public class EventBuilderTest {
    @Test
    public void anEvent() {
        EventBuilder.anEvent();
    }

    @Test
    public void withTimestamp() {
        Event event = EventBuilder.anEvent().withTimestamp(1L).build();
        assertNotNull(event);
    }

    @Test
    public void withEvent() {
        Event event = EventBuilder.anEvent().withTimestamp(1L).withEvent("test0815").build();
        assertNotNull(event);
    }

    @Test
    public void withParameters() {
        Map<String, Value> parameters = new HashMap<>();
        parameters.put("test", Value.of("test"));
        Event event = EventBuilder.anEvent()
                .withTimestamp(1L)
                .withEvent("test0815")
                .withParameters(parameters)
                .build();
        assertNotNull(event);
    }

    @Test
    public void withParameterString() {
        Event event = EventBuilder.anEvent()
                .withTimestamp(1L)
                .withEvent("test0815")
                .withParameter("string", "string")
                .build();
        assertNotNull(event);
    }

    @Test
    public void withParameterDouble() {
        Event event = EventBuilder.anEvent()
                .withTimestamp(1L)
                .withEvent("test0815")
                .withParameter("double", 0.1D)
                .build();
        assertNotNull(event);
    }

    @Test
    public void withParameterLong() {
        Event event = EventBuilder.anEvent()
                .withTimestamp(1L)
                .withEvent("test0815")
                .withParameter("long", 1L)
                .build();
        assertNotNull(event);
    }

    @Test
    public void withParameterDate() {
        Event event = EventBuilder.anEvent()
                .withTimestamp(1L)
                .withEvent("test0815")
                .withParameter("date", Date.from(Instant.ofEpochMilli(0L)))
                .build();
        assertNotNull(event);
    }

    @Test
    public void withParameterBoolean() {
        Event event = EventBuilder.anEvent()
                .withTimestamp(1L)
                .withEvent("test0815")
                .withParameter("boolean", false)
                .build();
        assertNotNull(event);
    }

    @Test
    public void withParameterValue() {
        Event event = EventBuilder.anEvent()
                .withTimestamp(1L)
                .withEvent("test0815")
                .withParameter("value", Value.of("value"))
                .build();
        assertNotNull(event);
    }
}