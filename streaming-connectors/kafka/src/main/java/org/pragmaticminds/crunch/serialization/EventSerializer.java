package org.pragmaticminds.crunch.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.pragmaticminds.crunch.events.Event;
import org.pragmaticminds.crunch.events.UntypedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Own serializer for {@link UntypedEvent}s
 */
public class EventSerializer implements Serializer<Event> {

    private static final Logger logger = LoggerFactory.getLogger(EventSerializer.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) { /* do nothing */}

    @Override
    public byte[] serialize(String topic, Event event) {
        try {
            UntypedEvent untypedEvent = UntypedEvent.fromEvent(event);
            return objectMapper.writeValueAsBytes(untypedEvent);
        } catch (JsonProcessingException e) {
            logger.error("could not deserialize Event", e);
            return new byte[0];
        }
    }

    @Override
    public void close() { /* do nothing */ }
}
