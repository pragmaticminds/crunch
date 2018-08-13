package org.pragmaticminds.crunch.events;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * - Create an Event as Example
 *
 * @author julian
 * Created by julian on 12.11.17
 */
public class EventTest {

    @Test
    public void builder_createEvent() {
        Event event = createEvent();

        assertEquals("LHL1", event.getSource());
    }

    private Event createEvent() {
        return EventBuilder.anEvent()
                .withTimestamp(1L)
                .withSource("LHL1")
                .withEvent("EVENT_NAME")
                .build();
    }

    @Test
    public void copyConstructor() {
        Event event = createEvent();
        Event event1 = new Event(event);
        assertEquals(event, event1);
    }
}