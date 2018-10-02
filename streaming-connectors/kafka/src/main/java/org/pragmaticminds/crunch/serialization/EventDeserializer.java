package org.pragmaticminds.crunch.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.pragmaticminds.crunch.events.GenericEvent;
import org.pragmaticminds.crunch.events.UntypedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Own deserializer for {@link UntypedEvent}s
 */
public class EventDeserializer implements Deserializer<GenericEvent> {
    private static final Logger logger = LoggerFactory.getLogger(EventDeserializer.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) { /* do nothing */}

    @Override
    public GenericEvent deserialize(String topic, byte[] data) {
        UntypedEvent event = null;
        try {
            event = objectMapper.readValue(data, UntypedEvent.class);
        } catch (IOException e) {
            logger.error("deserialization failed for object", e);
            return null;
        }
        return event.asEvent();
    }

    @Override
    public void close() { /* do nothing */ }

}
