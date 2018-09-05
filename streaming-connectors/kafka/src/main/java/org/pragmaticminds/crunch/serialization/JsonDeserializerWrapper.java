package org.pragmaticminds.crunch.serialization;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * This class wraps a {@link org.pragmaticminds.crunch.serialization.Deserializer} into a {@link Deserializer}, so that
 * it appears as the other kind.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 03.09.2018
 */
public class JsonDeserializerWrapper<T> implements Deserializer<T> {
    private final JsonDeserializer<T> innerDeserializer;
    
    /**
     * Main constructor, which takes the type class to deserialize
     * @param clazz to be deserialized
     */
    public JsonDeserializerWrapper(Class<T> clazz) {
        this.innerDeserializer = new JsonDeserializer<>(clazz);
    }
    
    /**
     * Configure this class.
     *
     * @param configs configs in key/value pairs
     * @param isKey   whether is for key or value
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        /* nothing to configure */
    }
    
    /**
     * Deserialize a record value from a byte array into a value or object.
     *
     * @param topic topic associated with the data
     * @param data  serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    @Override
    @SuppressWarnings("unchecked") // user must make sure the deserialization is to the right type
    public T deserialize(String topic, byte[] data) {
        return innerDeserializer.deserialize(data);
    }
    
    /**
     * pass close call to the inner deserializer
     */
    @Override
    public void close() {
        innerDeserializer.close();
    }
}
