/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.pragmaticminds.chronicle;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.ValueIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a simple program that writes some records to a Chronicle Queue and reads it back in.
 * As Basis for better understanding the Tool and the API.
 *
 * @author julian
 * Created by julian on 16.08.18
 */
public class WriteAndReadChronicleQueueNT {
    private static final Logger logger = LoggerFactory.getLogger(WriteAndReadChronicleQueueNT.class);

    public static final int NUMBER_OF_RECORDS = 10000;

    public static void main(String... ignored) {
        String basePath = System.getProperty("java.io.tmpdir") + "/SimpleChronicle";

        ChronicleQueue chronicleQueue = SingleChronicleQueueBuilder
                .single()
                .path(basePath)
                .build();

        // write objects
        ExcerptAppender appender = chronicleQueue.acquireAppender();
        for (int i = 1; i <= NUMBER_OF_RECORDS; i++) {
            int finalI = i;
            // Wrap the Message inside a "msg" tag to be flexible later to switch to different formats
            appender.writeDocument(w ->
                w.write(() -> "msg").text("Message number " + finalI)
            );
        }

        // read objects
        ExcerptTailer reader = chronicleQueue.createTailer().toStart();
        for (int i = 1; i <= NUMBER_OF_RECORDS; i++) {
            reader.readDocument(r -> {
                ValueIn read = r.read(() -> "msg");
                logger.info(read.text());
            });
        }

        chronicleQueue.close();
    }

}
