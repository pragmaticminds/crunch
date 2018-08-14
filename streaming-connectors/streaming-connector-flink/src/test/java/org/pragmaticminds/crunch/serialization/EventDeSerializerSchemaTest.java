package org.pragmaticminds.crunch.serialization;

import org.junit.Assert;
import org.junit.Test;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.Event;
import org.pragmaticminds.crunch.events.EventBuilder;


/**
 *
 *
 * @author julian
 * Created by julian on 14.08.18
 */
public class EventDeSerializerSchemaTest {

    @Test
    public void serAndDeser() {
        Event event = EventBuilder.anEvent()
                .withEvent("Type")
                .withTimestamp(1L)
                .withSource("me")
                .withParameter("a", Value.of(1L))
                .withParameter("b", Value.of("String"))
                .build();

        Event event1;
        EventSerializationSchema ser = new EventSerializationSchema();
        EventDeserializerSchema deSer = new EventDeserializerSchema();

        byte[] bytes = ser.serialize(event);
        event1 = deSer.deserialize(bytes);

        Assert.assertEquals(event, event1);
    }
    }