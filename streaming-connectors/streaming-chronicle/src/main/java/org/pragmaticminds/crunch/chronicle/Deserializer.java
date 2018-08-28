package org.pragmaticminds.crunch.chronicle;

import java.io.Serializable;

/**
 * An interface for converting bytes into object.
 *
 * @param <T> Type to be deserialized to
 * @author julian
 */
public interface Deserializer<T> extends AutoCloseable, Serializable {

    /**
     * Convert {@code data} into a byte array.
     *
     * @param bytes serilaized bytes
     * @return typed object
     */
    T deserialize(byte[] bytes);

    /**
     * Close this serializer.
     * <p>
     * This method must be idempotent as it may be called multiple times.
     */
    @Override
    void close();
}
