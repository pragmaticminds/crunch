package org.pragmaticminds.crunch.serialization;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Deserializes a JSON String to the given Class.
 *
 * @param <T> Type of Class
 * @author julian
 * Created by julian on 16.08.18
 */
public class JsonDeserializer<T> implements Deserializer<T> {

    private static final Logger logger = LoggerFactory.getLogger(JsonDeserializer.class);

    private final Class<T> clazz;
    private final ObjectMapper mapper;

    /**
     * Default Construcor
     *
     * @param clazz Clazz to deserialize to
     */
    public JsonDeserializer(Class<T> clazz) {
        this.clazz = clazz;
        mapper = new ObjectMapper(new JsonFactory());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public T deserialize(byte[] bytes) {
        try {
            return mapper.readValue(bytes, clazz);
        } catch (IOException e) {
            logger.warn("Unable to deserialize Object from byte array", e);
            return null;
        }
    }

    @Override
    public void close() {
        // Intentionally left blank
    }
}
