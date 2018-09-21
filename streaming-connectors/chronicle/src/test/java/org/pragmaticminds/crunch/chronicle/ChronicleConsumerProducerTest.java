package org.pragmaticminds.crunch.chronicle;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.chronicle.consumers.MemoryManager;
import org.pragmaticminds.crunch.serialization.Deserializer;
import org.pragmaticminds.crunch.serialization.JsonDeserializer;
import org.pragmaticminds.crunch.serialization.JsonSerializer;
import org.pragmaticminds.crunch.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * @author julian
 * Created by julian on 16.08.18
 */
public class ChronicleConsumerProducerTest {

    private static final Logger logger = LoggerFactory.getLogger(ChronicleConsumerProducerTest.class);

    @Test
    public void produceAndConsume_waitFornewMessage_blockNotNPE() throws Exception {
        String basePath = System.getProperty("java.io.tmpdir");
        String path = Files.createTempDirectory(Paths.get(basePath), "chronicle-")
                .toAbsolutePath()
                .toString();
        logger.info("Using temp path '{}'", path);

        Properties properties = new Properties();
        properties.put(ChronicleConsumer.CHRONICLE_PATH_KEY, path);
        properties.put(ChronicleConsumer.CHRONICLE_CONSUMER_KEY, "asdf");

        // Create before, because moves to end
        Thread thread;
        try (ChronicleConsumer<UntypedValues> consumer = new ChronicleConsumer<>(properties, new MemoryManager(), new JsonDeserializer<>(UntypedValues.class))) {
            try (ChronicleProducer<UntypedValues> producer = new ChronicleProducer<>(properties, new JsonSerializer<>())) {

                // Write
                UntypedValues values = UntypedValues.builder()
                        .prefix("")
                        .source("test")
                        .timestamp(Instant.now().toEpochMilli())
                        .values(Collections.singletonMap("key", "Julian"))
                        .build();

                producer.send(values);

                // Read
                consumer.poll();

                // Do a second read which "waits" until another value is written
                thread = new Thread(() -> {
                    // Fail the test after two seconds

                    try {
                        Thread.sleep(2_00);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    try (ChronicleProducer<UntypedValues> producer2 = new ChronicleProducer<>(properties, new JsonSerializer<>())) {
                        producer2.send(values);
                    }
                    try {
                        Thread.sleep(2_000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    fail("Did not read the record");
                });

                // Start background thread that writes after 2 seconds
                thread.start();
                UntypedValues poll = consumer.poll();
                thread.interrupt();
                // Assert not null
                assertNull(poll);
            }
        }
    }

    @Test
    public void produceAndConsume_manyMessages() throws Exception {
        String basePath = System.getProperty("java.io.tmpdir");
        String path = Files.createTempDirectory(Paths.get(basePath), "chronicle-")
                .toAbsolutePath()
                .toString();
        logger.info("Using temp path '{}'", path);

        Properties properties = new Properties();
        properties.put(ChronicleConsumer.CHRONICLE_PATH_KEY, path);
        properties.put(ChronicleConsumer.CHRONICLE_CONSUMER_KEY, "asdf");

        // Create before, because moves to end
        int counter;
        try (ChronicleConsumer<UntypedValues> consumer = new ChronicleConsumer<>(properties, new MemoryManager(), new JsonDeserializer<>(UntypedValues.class))) {
            try (ChronicleProducer<UntypedValues> producer = new ChronicleProducer<>(properties, new JsonSerializer<>())) {

                // Write
                for (int i = 0; i <= 1_000; i++) {
                    logger.debug("Writing {}", i);
                    assertTrue(producer.send(
                            UntypedValues.builder()
                                    .prefix("")
                                    .source("test")
                                    .timestamp(Instant.now().toEpochMilli())
                                    .values(Collections.singletonMap("key", i))
                                    .build()
                    ));
                }
                // Read
                counter = 0;
                for (int i = 0; i <= 1_000; i++) {
                    logger.debug("Reading {}", i);
                    consumer.poll();
                    counter++;
                }
            }
        }
        assertEquals(1_001, counter);
    }

    // Test with custom POJO

    @Test
    public void produceAndConsume() throws Exception {
        String basePath = System.getProperty("java.io.tmpdir");
        String path = Files.createTempDirectory(Paths.get(basePath), "chronicle-")
                .toAbsolutePath()
                .toString();
        logger.info("Using temp path '{}'", path);

        Properties properties = new Properties();
        properties.put(ChronicleConsumer.CHRONICLE_PATH_KEY, path);
        properties.put(ChronicleConsumer.CHRONICLE_CONSUMER_KEY, "asdf");

        ObjectMapper mapper = new ObjectMapper(new JsonFactory());
        Serializer<MyPojo> serializer = getSerializer(mapper);
        Deserializer<MyPojo> deserializer = getDeserializer(mapper);

        // Create before, because moves to end
        MyPojo pojo;
        try (ChronicleConsumer<MyPojo> consumer = new ChronicleConsumer<>(properties, new MemoryManager(), deserializer)) {
            try (ChronicleProducer<MyPojo> producer = new ChronicleProducer<>(properties, serializer)) {
                // Write
                assertTrue(producer.send(new MyPojo("Julian", 123)));
                // Read
                pojo = consumer.poll();
            }
        }

        assertEquals("Julian", pojo.getItem1());
        assertEquals(123, pojo.getItem2());
    }

    @NotNull
    private Serializer<MyPojo> getSerializer(ObjectMapper mapper) {
        return new Serializer<MyPojo>() {
            @Override
            public byte[] serialize(MyPojo data) {
                try {
                    return mapper.writeValueAsBytes(data);
                } catch (JsonProcessingException e) {
                    return new byte[0];
                }
            }

            @Override
            public void close() {
                // do nothing.
            }
        };
    }

    @NotNull
    private Deserializer<MyPojo> getDeserializer(ObjectMapper mapper) {
        return new Deserializer<MyPojo>() {
            @Override
            public MyPojo deserialize(byte[] bytes) {
                try {
                    return mapper.readValue(bytes, MyPojo.class);
                } catch (IOException e) {
                    return null;
                }
            }

            @Override
            public void close() {
                // Do nothing
            }
        };
    }

    public static class MyPojo {
        private String item1;
        private int item2;

        public MyPojo() {
            // Default for Jackson
        }

        public MyPojo(String item1, int item2) {
            this.item1 = item1;
            this.item2 = item2;
        }

        public String getItem1() {
            return item1;
        }

        public int getItem2() {
            return item2;
        }
    }
}