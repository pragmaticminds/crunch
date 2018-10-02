package org.pragmaticminds.crunch.events;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * - Create an GenericEvent as Example
 *
 * @author julian
 * Created by julian on 12.11.17
 */
public class EventTest {

    @Test
    public void builder_createEvent() {
        GenericEvent event = createEvent();

        assertEquals("LHL1", event.getSource());
    }

    private GenericEvent createEvent() {
        return GenericEventBuilder.anEvent()
                .withTimestamp(1L)
                .withSource("LHL1")
                .withEvent("EVENT_NAME")
                .build();
    }

    @Test
    public void copyConstructor() {
        GenericEvent event = createEvent();
        GenericEvent event1 = new GenericEvent(event);
        assertEquals(event, event1);
    }
}