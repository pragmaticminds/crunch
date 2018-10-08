package org.pragmaticminds.crunch.execution;

import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.events.GenericEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author julian
 * @author Erwin Wagasow
 * Created by julian on 15.08.18
 */
public class EventSinkContextTest {

    @Test
    public void testCollect() {
        EventSink<GenericEvent> mock = Mockito.mock(EventSink.class);
        EventSinkContext<GenericEvent> context = new EventSinkContext<>(mock);

        // Check getter and setter
        UntypedValues current = new UntypedValues();
        context.setCurrent(current);
        MRecord record = context.get();

        assertEquals(current, record);

        // Call collect
        GenericEvent event = new GenericEvent();
        context.collect(event);

        // Assert that it is called.
        Mockito.verify(mock).handle(ArgumentMatchers.eq(event));
    }
    
    @Test
    public void serializable() {
        EventSinkContext<GenericEvent> context = new EventSinkContext<>(new InnerEventSink());
        assertNotNull(ClonerUtil.clone(context));
    }
    
    public static class InnerEventSink implements EventSink<GenericEvent> {
        private static final Logger logger = LoggerFactory.getLogger(InnerEventSink.class);
        @Override
        public void handle(GenericEvent event) {
            logger.trace("event: {}", event);
        }
    }
}