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

package org.pragmaticminds.crunch.chronicle;

import com.google.common.base.Preconditions;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.pragmaticminds.crunch.serialization.Serializer;
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

    private final Serializer<T>   serializer;
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

        logger.info("Starting Chronicle Producer for folder {}", path);

        chronicleQueue = SingleChronicleQueueBuilder
                .single()
                .path(path)
                .build();

        appender = chronicleQueue.acquireAppender();
    }

    public boolean send(T value) {
        byte[] bytes = serializer.serialize(value);
        try (final DocumentContext dc = appender.writingDocument()) {
            dc.wire().write(() -> "msg").bytes(bytes);
            if (logger.isTraceEnabled()) {
                logger.trace("Data {} was store to index {}", new String(bytes), dc.index());
            }
            return true;
        } catch (Exception e) {
            logger.warn("Unable to store value " + new String(bytes) + " to chronicle", e);
            return false;
        }
    }

    @Override
    public void close() {
        logger.info("Closing Chronicle Producer");
        chronicleQueue.close();
    }
}
