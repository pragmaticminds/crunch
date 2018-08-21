package org.pragmaticminds.crunch.chronicle;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serializes Objects to JSON
 *
 * @author julian
 * Created by julian on 16.08.18
 */
public class JsonSerializer<T> implements Serializer<T> {

    private static final Logger logger = LoggerFactory.getLogger(JsonSerializer.class);

    private final ObjectMapper mapper;

    public JsonSerializer() {
        mapper = new ObjectMapper(new JsonFactory());
    }

    @Override
    public byte[] serialize(T data) {
        try {
            return mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            logger.warn("Unable to serialize the given object " + data, e);
            return new byte[0];
        }
    }

    @Override
    public void close() {
        // Do nothing
    }
}
