package org.pragmaticminds.crunch.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.pragmaticminds.crunch.events.GenericEvent;
import org.pragmaticminds.crunch.events.UntypedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * defines the Deserialization schema for GenericEvent-Datatype based on {@link EventDeserializer} for usage with Kafka
 * Created by timbo on 24.11.17
 */
public class EventDeserializerSchema extends AbstractDeserializationSchema<GenericEvent> {
    private static final Logger logger = LoggerFactory.getLogger(EventDeserializerSchema.class);

    private final ObjectMapper objectMapper;

    /**
     * default constructor
     */
    public EventDeserializerSchema() {
        objectMapper = new ObjectMapper();
    }

    /**
     * De-serializes the byte message.
     *
     * @param message The message, as a byte array.
     * @return The de-serialized message as an object.
     */
    @Override
    public GenericEvent deserialize(byte[] message) {
        UntypedEvent event;
        try {
            event = objectMapper.readValue(message, UntypedEvent.class);
        } catch (IOException e) {
            logger.error("deserialization failed for object", e);
            return null;
        }
        return event.asEvent();
    }

    /**
     * returns the type information for the deserialized data.
     * IMPORTANT: without this function the code fails with an UntypedError Message, because Java could not determine the type of the deserialized data
     *
     * @return type information of deserialized data
     */
    @Override
    public TypeInformation<GenericEvent> getProducedType() {
        return TypeInformation.of(GenericEvent.class);
    }
}
