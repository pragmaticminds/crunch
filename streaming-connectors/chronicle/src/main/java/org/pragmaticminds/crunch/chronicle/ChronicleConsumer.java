package org.pragmaticminds.crunch.chronicle;

import com.google.common.base.Preconditions;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.pragmaticminds.crunch.chronicle.consumers.ConsumerManager;
import org.pragmaticminds.crunch.chronicle.consumers.JdbcConsumerManager;
import org.pragmaticminds.crunch.serialization.Deserializer;

import java.io.Serializable;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Consumes Records from a Chronicle Queue.
 *
 * @author julian
 * Created by julian on 16.08.18
 */
public class ChronicleConsumer<T> implements AutoCloseable, Serializable {

    public static final String CHRONICLE_PATH_KEY = "chronicle.path";
    public static final String CHRONICLE_CONSUMER_KEY = "chronicle.consumer";

    private final transient ChronicleQueue chronicleQueue;
    private final transient ExcerptTailer tailer;
    private final ConsumerManager manager;
    private final String consumer;
    private final Deserializer<T> deserializer;
    // Offset of the last "read" record
    private long currentOffset;

    /**
     * Creates a Chronicle Consumer with the given Properties.
     *
     * The necessary Properties are
     * <ul>
     *     <li>chronicle.path</li>
     *     <li>chronicle.consumer</li>
     * </ul>
     *
     * The Constants CHRONICLE_PATH_KEY and CHRONICLE_CONSUMER_KEY
     * can be used for that.
     *
     * @param properties Properties to use
     * @param manager ConsumerManager to manage Consumers
     * @param deserializer Deserializer to use
     */
    ChronicleConsumer(Properties properties, ConsumerManager manager, Deserializer<T> deserializer) {
        Preconditions.checkArgument(properties.containsKey(CHRONICLE_PATH_KEY),
                "No chronicle path given.");
        Preconditions.checkArgument(properties.containsKey(CHRONICLE_CONSUMER_KEY),
                "No chronicle consumer given.");
        Preconditions.checkNotNull(deserializer);
        Preconditions.checkNotNull(manager);

        this.deserializer = deserializer;
        this.manager = manager;
        consumer = properties.getProperty(CHRONICLE_CONSUMER_KEY);

        String path = properties.getProperty(CHRONICLE_PATH_KEY);

        chronicleQueue = ChronicleQueueBuilder
                .single()
                .path(path)
                .build();

        tailer = chronicleQueue.createTailer();

        // Set tailer to the current offset for this group
        long offset = manager.getOffset(consumer);
        // If offset -1 set to start
        if (offset == -1L) {
            tailer.toStart();
        } else {
            tailer.moveToIndex(offset);
        }
        currentOffset = tailer.index();
    }

    /**
     * Builder for a Chronicle Consumer.
     *
     * @return New builders
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Package private getters for testing
     */
    ConsumerManager getManager() {
        return manager;
    }

    Deserializer<T> getDeserializer() {
        return deserializer;
    }

    /**
     * Fetch next value from the Queue. Blocks until the next value is received.
     *
     * @return Value
     */
    public T poll() {
        // Acknowledge last read, i.e., set the stored index +1
        manager.acknowledgeOffset(consumer, currentOffset);
        // Skip until we read Data.
        DocumentContext documentContext;
        do {
            documentContext = tailer.readingDocument();
            currentOffset = documentContext.index();
        } while (!documentContext.isData());

        // Extract the wire and assure it is not null
        Wire wire = documentContext.wire();
        // TODO jf 21.08.18: Is this the right way to handle this case?
        // Perhaps we should throw a dedicated Exception?
        // Perhaps throw no Exception and return null?
        Preconditions.checkNotNull(wire);

        byte[] bytes = wire
                .read("msg")
                .bytes();

        return this.deserializer.deserialize(bytes);
    }

    @Override
    public void close() throws Exception {
        chronicleQueue.close();
    }


    /**
     * Internal Builder for the Chronicle Consumer
     */
    public static final class Builder<T> {

        private String path;
        private String name;
        private ConsumerManager manager;
        private Deserializer<T> deserializer;

        private Builder() {
            // Nothing needed here
        }

        public Builder<T> withPath(String path) {
            this.path = path;
            return this;
        }

        public Builder<T> withConsumerName(String consumerName) {
            this.name = consumerName;
            return this;
        }

        /**
         * Optional. Otherwise a {@link JdbcConsumerManager} will be used as default.
         *
         * @param manager manager to use
         * @return this instance
         */
        public Builder<T> withManager(ConsumerManager manager) {
            this.manager = manager;
            return this;
        }

        public Builder<T> withDeserializer(Deserializer<T> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public ChronicleConsumer<T> build() {
            Properties properties = new Properties();
            properties.put(CHRONICLE_PATH_KEY, path);
            properties.put(CHRONICLE_CONSUMER_KEY, name);
            if (manager == null) {
                return new ChronicleConsumer<>(properties, new JdbcConsumerManager(Paths.get(path)), deserializer);
            } else {
                return new ChronicleConsumer<>(properties, manager, deserializer);
            }
        }
    }
}
