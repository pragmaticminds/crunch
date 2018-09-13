package org.pragmaticminds.crunch.chronicle;


import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.execution.MRecordSource;
import org.pragmaticminds.crunch.serialization.JsonDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Wrapper around the {@link ChronicleConsumer} exposing a {@link MRecordSource} interface for usage in Pipelines.
 * <p>
 * The connection to the Chronicle Queue on the FS is done in the {@link #init()} method.
 * Thus this is serializable and can be instantiated multiple times.
 *
 * @author julian
 * Created by julian on 21.08.18
 */
public class ChronicleSource implements MRecordSource {

    private static final Logger logger = LoggerFactory.getLogger(ChronicleSource.class);

    private final ChronicleConsumerFactory consumerFactory;
    private ChronicleConsumer<UntypedValues> consumer;

    /**
     * Creates a Source which instantiates a default consumer.
     *
     * @param path         path to the chronicle queue
     * @param consumerName Name for the consumer to use
     */
    public ChronicleSource(String path, String consumerName) {
        consumerFactory = new ChronicleConsumerFactoryImpl(path, consumerName);
    }

    /**
     * For fine grained control or testing.
     *
     * @param consumerFactory Factory to build the consumer.
     */
    public ChronicleSource(ChronicleConsumerFactory consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    @Override
    public MRecord get() {
        return consumer.poll();
    }

    @Override
    public boolean hasRemaining() {
        return true;
    }

    @Override
    public void init() {
        // Instantiate the Consumer
        this.consumer = consumerFactory.create();
    }

    @Override
    public void close() {
        try {
            consumer.close();
        } catch (Exception e) {
            logger.debug("Exception during closing ChronicleConsumer", e);
        }
    }

    @Override
    public Kind getKind() {
        return Kind.INFINITE;
    }

    /**
     * Factory for testing.
     */
    @FunctionalInterface
    interface ChronicleConsumerFactory extends Serializable {

        ChronicleConsumer<UntypedValues> create();

    }

    /**
     * Default implementation of {@link ChronicleConsumerFactory}
     */
    private class ChronicleConsumerFactoryImpl implements ChronicleConsumerFactory {

        private final String path;
        private final String consumerName;

        public ChronicleConsumerFactoryImpl(String path, String consumerName) {

            this.path = path;
            this.consumerName = consumerName;
        }

        @Override
        public ChronicleConsumer<UntypedValues> create() {
            return ChronicleConsumer.<UntypedValues>builder()
                    .withPath(path)
                    .withConsumerName(consumerName)
                    .withDeserializer(new JsonDeserializer<>(UntypedValues.class))
                    .build();
        }
    }

}
