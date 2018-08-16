package org.pragmaticminds.chronicle;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.ValueIn;

/**
 * This is a simple program that writes some records to a Chronicle Queue and reads it back in.
 * As Basis for better understanding the Tool and the API.
 *
 * @author julian
 * Created by julian on 16.08.18
 */
public class WriteAndReadChronicleQueueNT {

    public static final int NUMBER_OF_RECORDS = 10000;

    public static void main(String... ignored) {
        String basePath = System.getProperty("java.io.tmpdir") + "/SimpleChronicle";

        ChronicleQueue chronicleQueue = ChronicleQueueBuilder
                .single()
                .path(basePath)
                .build();

        // write objects
        ExcerptAppender appender = chronicleQueue.acquireAppender();
        for (int i = 1; i <= NUMBER_OF_RECORDS; i++) {
            int finalI = i;
            // Wrap the Message inside a "msg" tag to be flexible later to switch to different formats
            appender.writeDocument(w -> {
                w.write(() -> "msg").text("Message number " + finalI);
            });
        }

        // read objects
        ExcerptTailer reader = chronicleQueue.createTailer().toStart();
        for (int i = 1; i <= NUMBER_OF_RECORDS; i++) {
            reader.readDocument(r -> {
                ValueIn read = r.read(() -> "msg");
                System.out.println(read.text());
            });
        }

        chronicleQueue.close();
    }

}
