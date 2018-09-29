package org.pragmaticminds.crunch.chronicle;

import com.google.common.base.Preconditions;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.pragmaticminds.crunch.chronicle.consumers.ConsumerManager;
import org.pragmaticminds.crunch.chronicle.consumers.JdbcConsumerManager;
import org.pragmaticminds.crunch.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger logger = LoggerFactory.getLogger(ChronicleConsumer.class);

    public static final String CHRONICLE_PATH_KEY = "chronicle.path";
    public static final String CHRONICLE_CONSUMER_KEY = "chronicle.consumer";

    private  transient ChronicleQueue chronicleQueue;
    private  transient ExcerptTailer tailer;
    private final ConsumerManager manager;
    private final String consumer;
    private final Deserializer<T> deserializer;
    // Offset of the last "read" record
    private long currentIndex;
    private long lastIndex;
    private String path;


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

        this.path = properties.getProperty(CHRONICLE_PATH_KEY);

        logger.info("Starting Chronicle Consumer with group {} in path {}", consumer, path);

        createQueue();
    }

    /**
     * Creates a new queue. Before calling this method, an existing queue on the same path has to be closed.
     */
    private void createQueue() {
        chronicleQueue = SingleChronicleQueueBuilder
                .single()
                .path(path)
                .build();

        tailer = chronicleQueue.createTailer();

        // Set tailer to the current offset for this group
        long offset = manager.getOffset(consumer);
        if(logger.isDebugEnabled()) {
            logger.debug("Fetching offset for consumer {}, offset {}", consumer, offset);
        }
        // If offset -1 set to start
        if (offset == -1L) {
            if(logger.isDebugEnabled()) {
                logger.debug("Resetting offset for consumer {} to start", consumer);
            }
            tailer.toStart();
        } else {
            tailer.moveToIndex(offset);
            if(logger.isDebugEnabled()) {
                logger.debug("Setting offset for consumer {} to {}", consumer, offset);
            }
        }

        currentIndex = tailer.index();
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
        manager.acknowledgeOffset(consumer, currentIndex, true);
        // Fetch records until we read a non null entry
        return fetchNextRecord();
    }

    /**
     * Closes and re-creates the queue.
     */
    private void reinitialize() {
        this.close();
        createQueue();
    }

    /**
     * Fetches the next record from the chronicle.
     * Can in some situations return null.
     *
     * @return Next Record, possibly null
     */
    private T fetchNextRecord() {
        try (DocumentContext documentContext = tailer.readingDocument()) {
            currentIndex = documentContext.index();

            // save the last valid index. When index becomes invalid, reinitialize queue
            if (currentIndex > 0) {
                lastIndex = currentIndex;
            } else {
                currentIndex = lastIndex +1;
                manager.acknowledgeOffset(consumer, currentIndex, false);

                //reinitialize
                reinitialize();
            }

            logger.trace("Current offset is {}", currentIndex);

            // Extract the wire and assure it is not null
            Wire wire = documentContext.wire();

            // End of document, wire should be not null otherwise
            if (!documentContext.isData()) {
                return null;
            }

            String msg = wire
                    .read("msg")
                    .text();

            logger.trace("Current offset is {} record is {}", currentIndex, msg);

            if (msg == null) {
                // Forces to skip these situations
                return null;
            } else {
                return this.deserializer.deserialize(msg.getBytes());
            }
        }
    }

    @Override
    public void close() {
        logger.info("Closing Chronicle Consumer");
        chronicleQueue.close();
    }


    /**
     * Internal Builder for the Chronicle Consumer
     */
    public static final class Builder<T> {

        private String path;
        private String name;
        private Long acknowledgementRate;
        private ConsumerManager manager;
        private Deserializer<T> deserializer;

        private Builder() {
            this.acknowledgementRate=null;
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

        public Builder<T> withAcknowledgementRate(Long acknowledgementRate) {
            if(acknowledgementRate!=null && acknowledgementRate>0) {
                //only positive not 0 acknowledgement rate are plausible, otherwise fall back to default
                this.acknowledgementRate = acknowledgementRate;
            }
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
                if(acknowledgementRate==null){
                    return new ChronicleConsumer<>(properties, new JdbcConsumerManager(Paths.get(path)), deserializer);
                }
                else{
                    return new ChronicleConsumer<>(properties, new JdbcConsumerManager(Paths.get(path),acknowledgementRate), deserializer);
                }
            } else {
                return new ChronicleConsumer<>(properties, manager, deserializer);
            }
        }
    }
}
