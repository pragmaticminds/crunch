package org.pragmaticminds.crunch.chronicle;

import com.google.common.base.Preconditions;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.pragmaticminds.crunch.chronicle.ChronicleConsumer.CHRONICLE_PATH_KEY;

/**
 * Consumes Records from a Chronicle Queue.
 *
 * @author julian
 * Created by julian on 16.08.18
 */
public class ChronicleProducer<T> implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ChronicleProducer.class);

    private final ChronicleQueue chronicleQueue;

    private final Serializer<T> serializer;
    private final ExcerptAppender appender;

    /**
     * Creates a Chronicle Consumer with the given Properties.
     *
     * @param properties
     */
    public ChronicleProducer(Properties properties, Serializer<T> serializer) {
        Preconditions.checkArgument(properties.containsKey(CHRONICLE_PATH_KEY),
                "No chronicle path given.");
        Preconditions.checkNotNull(serializer);

        this.serializer = serializer;

        String path = properties.getProperty(CHRONICLE_PATH_KEY);

        chronicleQueue = ChronicleQueueBuilder
                .single()
                .path(path)
                .build();

        appender = chronicleQueue.acquireAppender();
    }

    public boolean send(T value) {
        byte[] bytes = serializer.serialize(value);
        try {
            appender.writeDocument(w -> w.write(() -> "msg").bytes(bytes));
            return true;
        } catch (Exception e) {
            logger.warn("Unable to store value to chronicle", e);
            return false;
        }
    }

    @Override
    public void close() throws Exception {
        chronicleQueue.close();
    }
}
