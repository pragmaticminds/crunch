package org.pragmaticminds.crunch.chronicle;

/**
 * An interface for converting objects to bytes.
 *
 * @param <T> Type to be serialized from.
 * @author julian
 */
public interface Serializer<T> extends AutoCloseable {

    /**
     * Convert {@code data} into a byte array.
     *
     * @param data typed data
     * @return serialized bytes
     */
    byte[] serialize(T data);

    /**
     * Close this serializer.
     * <p>
     * This method must be idempotent as it may be called multiple times.
     */
    @Override
    void close();
}
