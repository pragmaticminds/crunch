package org.pragmaticminds.crunch.chronicle;

import org.junit.Test;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.serialization.JsonSerializer;

import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

import static org.pragmaticminds.crunch.chronicle.ChronicleConsumer.CHRONICLE_PATH_KEY;

/**
 * @author julian
 * Created by julian on 12.09.18
 */
public class ChronicleConsumerMT {

    public static final String TMP_CHRONICLE = "/tmp/chronicle";

    /**
     * Starts on an existing chronicle and after 10 seconds pushes more to it.
     */
    @Test
    public void testRealWorldData() {
        Properties properties = new Properties();
        properties.setProperty(CHRONICLE_PATH_KEY, TMP_CHRONICLE);
        ChronicleProducer<UntypedValues> producer = new ChronicleProducer<>(properties, new JsonSerializer<>());

        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(10_000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while (true) {
                producer.send(
                        UntypedValues.builder()
                                .source("asdf")
                                .prefix("")
                                .timestamp(Instant.now().toEpochMilli())
                                .values(Collections.singletonMap("a", "b"))
                                .build()
                );
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        thread.start();

        ChronicleSource chronicleSource = new ChronicleSource(TMP_CHRONICLE, "consumer");

        chronicleSource.init();

        while (chronicleSource.hasRemaining()) {
            MRecord record = chronicleSource.get();

            System.out.println(record);
        }

        chronicleSource.close();
    }
}