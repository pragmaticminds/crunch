package org.pragmaticminds.crunch.chronicle;

import org.junit.Test;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.chronicle.consumers.ConsumerManager;
import org.pragmaticminds.crunch.chronicle.consumers.JdbcConsumerManager;
import org.pragmaticminds.crunch.serialization.JsonDeserializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Add some tests for the Builder.
 *
 * @author julian
 * Created by julian on 21.08.18
 */
public class ChronicleConsumerTest {

    @Test
    public void buildWithBuilder_noManager() {
        JsonDeserializer<MRecord> deserializer = new JsonDeserializer<>(MRecord.class);

        ChronicleConsumer<MRecord> consumer = ChronicleConsumer.<MRecord>builder()
                .withConsumerName("name")
                .withPath("/tmp")
                .withDeserializer(deserializer)
                .build();

        // Assert it took the right deserializer
        assertEquals(deserializer, consumer.getDeserializer());
        // Assert it generated a JdbcManager
        assertTrue(consumer.getManager() instanceof JdbcConsumerManager);
    }

    @Test
    public void buildWithBuilder_withManager() {
        JsonDeserializer<MRecord> deserializer = new JsonDeserializer<>(MRecord.class);
        ConsumerManager manager = getManager();

        ChronicleConsumer<MRecord> consumer = ChronicleConsumer.<MRecord>builder()
                .withConsumerName("name")
                .withPath("/tmp")
                .withDeserializer(deserializer)
                .withManager(manager)
                .build();

        // Assert it took the right deserializer
        assertEquals(deserializer, consumer.getDeserializer());
        // Assert it used right manager
        assertEquals(manager, consumer.getManager());
    }

    /**
     * Dummy manager.
     */
    private ConsumerManager getManager() {
        return new ConsumerManager() {

            @Override
            public void close() {
            }

            @Override
            public long getOffset(String consumer) {
                return 0;
            }

            @Override
            public void acknowledgeOffset(String consumer, long offset, boolean useAcknowledgeRate) {
            }
        };
    }
}