package org.pragmaticminds.crunch.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.pragmaticminds.crunch.events.Event;
import org.pragmaticminds.crunch.events.UntypedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a schema that serializes an Event datatype
 * Created by timbo on 24.11.17
 */
public class EventSerializationSchema implements SerializationSchema<Event> {

    private static final Logger logger = LoggerFactory.getLogger(EventSerializationSchema.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public EventSerializationSchema() {
        //sonar wants it that way
    }

    /**
     * serializes an Event to a byte array
     *
     * @param element the Event that shall be serialized
     * @return the equivalent serialized byte array
     */
    static byte[] serializeEvent(Event element) {
        try {
            UntypedEvent untypedEvent = UntypedEvent.fromEvent(element);
            return objectMapper.writeValueAsBytes(untypedEvent);
        } catch (JsonProcessingException e) {
            logger.error("could not deserialize Event", e);
            return new byte[0];
        }
    }

    /**
     * Serializes the incoming element to a specified type.
     *
     * @param element The incoming element to be serialized
     * @return The serialized element.
     */
    @Override
    public byte[] serialize(Event element) {
        return EventSerializationSchema.serializeEvent(element);
    }
}
