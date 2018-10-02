package org.pragmaticminds.crunch.serialization;

import org.junit.Assert;
import org.junit.Test;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.GenericEvent;
import org.pragmaticminds.crunch.events.GenericEventBuilder;

/**
 *
 *
 * @author julian
 * Created by julian on 14.08.18
 */
public class EventDeSerializerTest {

    @Test
    public void deSerializeEvent() {
        GenericEvent event = GenericEventBuilder.anEvent()
                .withEvent("Type")
                .withTimestamp(1L)
                .withSource("me")
                .withParameter("a", Value.of(1L))
                .withParameter("b", Value.of("String"))
                .build();

        GenericEvent event1;
        try (EventSerializer ser = new EventSerializer()) {
            try (EventDeserializer deSer = new EventDeserializer()) {

                byte[] bytes = ser.serialize("", event);
                event1 = deSer.deserialize("", bytes);
            }
        }

        Assert.assertEquals(event, event1);
    }
}