package org.pragmaticminds.crunch.serialization;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * This class wraps the {@link org.pragmaticminds.crunch.serialization.Serializer} into a {@link Serializer}.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 04.09.2018
 */
public class JsonSerializerWrapper<T> implements Serializer<T> {
    
    private org.pragmaticminds.crunch.serialization.Serializer serializer;
    
    public JsonSerializerWrapper() {
        serializer = new JsonSerializer();
    }
    
    /**
     * Configure this class.
     *
     * @param configs configs in key/value pairs
     * @param isKey   whether is for key or value
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        /* nothing to do in here */
    }
    
    /**
     * Convert {@code data} into a byte array.
     *
     * @param topic topic associated with data
     * @param data  typed data
     * @return serialized bytes
     */
    @Override
    public byte[] serialize(String topic, T data) {
        return serializer.serialize(data);
    }
    
    /**
     * Close this serializer.
     * <p>
     * This method must be idempotent as it may be called multiple times.
     */
    @Override
    public void close() {
        serializer.close();
    }
}
