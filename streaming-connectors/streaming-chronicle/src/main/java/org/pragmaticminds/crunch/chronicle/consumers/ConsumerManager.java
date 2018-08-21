package org.pragmaticminds.crunch.chronicle.consumers;

/**
 * Stores Offsets and returns them on request
 *
 * @author julian
 * Created by julian on 16.08.18
 */
public interface ConsumerManager extends AutoCloseable {

    /**
     * Returns the offset associated with the given consumer
     *
     * @param consumer Name of the consumer
     * @return Offset of the consumer or -1 if no offset is stored
     */
    long getOffset(String consumer);

    /**
     * Acknowledges, i.e., stores the offset persistend
     *
     * @param consumer name of the consumer
     * @param offset   offset to store
     */
    void acknowledgeOffset(String consumer, long offset);
}
