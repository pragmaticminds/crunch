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
     * Creates a Source which instantiates a default consumer with a custom acknowledgementRate
     *
     * @param path                  path to the chronicle queue
     * @param consumerName          Name for the consumer to use
     * @param acknowledgementRate   AcknowledgemntRate that indicates every xth offset will be commited
     */
    public ChronicleSource(String path, String consumerName,Long acknowledgementRate) {
        consumerFactory = new ChronicleConsumerFactoryImpl(path, consumerName,acknowledgementRate);
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
        private final Long acknowledgementRate;

        ChronicleConsumerFactoryImpl(String path, String consumerName) {
            this(path,consumerName,null);
        }

        ChronicleConsumerFactoryImpl(String path, String consumerName,Long acknowledgementRate) {
            this.path = path;
            this.consumerName = consumerName;
            this.acknowledgementRate = acknowledgementRate;
        }

        @Override
        public ChronicleConsumer<UntypedValues> create() {
            return ChronicleConsumer.<UntypedValues>builder()
                    .withPath(path)
                    .withConsumerName(consumerName)
                    .withAcknowledgementRate(acknowledgementRate)
                    .withDeserializer(new JsonDeserializer<>(UntypedValues.class))
                    .build();
        }
    }

}
