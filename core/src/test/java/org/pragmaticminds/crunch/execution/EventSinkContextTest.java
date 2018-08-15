package org.pragmaticminds.crunch.execution;

import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.events.Event;

import static org.junit.Assert.assertEquals;

/**
 * @author julian
 * Created by julian on 15.08.18
 */
public class EventSinkContextTest {

    @Test
    public void testCollect() {
        EventSink mock = Mockito.mock(EventSink.class);
        EventSinkContext context = new EventSinkContext(mock);

        // Check getter and setter
        UntypedValues current = new UntypedValues();
        context.setCurrent(current);
        MRecord record = context.get();

        assertEquals(current, record);

        // Call collect
        Event event = new Event();
        context.collect(event);

        // Assert that it is called.
        Mockito.verify(mock).handle(ArgumentMatchers.eq(event));
    }
}