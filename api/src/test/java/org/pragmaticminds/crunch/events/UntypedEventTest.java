package org.pragmaticminds.crunch.events;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.values.dates.Value;

import javax.management.openmbean.InvalidKeyException;
import java.sql.Date;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests the conversions from and to {@link UntypedEvent} and {@link Event}
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 17.11.2017
 */
public class UntypedEventTest {
    private final Value testString = Value.of("testString");
    private final Value testDouble = Value.of((Double) 0.1);
    private final Value testLong = Value.of(1L);
    private final Value testDate = Value.of(Date.from(Instant.now()));
    private final Value testBoolean = Value.of(true);
    private Event event;
    private Map<String, Value> parameters;

    @Before
    public void setUp() {
        parameters = new HashMap<>();
        parameters.put("string", testString);
        parameters.put("double", testDouble);
        parameters.put("long", testLong);
        parameters.put("date", testDate);
        parameters.put("boolean", testBoolean);
        event = new Event(1L, "testEvent", "testSource", parameters);
    }

    @Test
    public void fromEventAndAsEvent() {
        UntypedEvent untypedEvent = UntypedEvent.fromEvent(event);

        Event resultEvent = untypedEvent.asEvent();

        assertEquals(event, resultEvent);
    }

    @Test
    public void getParameter() {
        assertNotNull(event.getParameter("string"));
        assertNotNull(event.getParameter("double"));
        assertNotNull(event.getParameter("long"));
        assertNotNull(event.getParameter("date"));
        assertNotNull(event.getParameter("boolean"));
    }

    @Test(expected = InvalidKeyException.class)
    public void getParameterNotInParameters() {
        event.getParameter("notPresent");
    }
}