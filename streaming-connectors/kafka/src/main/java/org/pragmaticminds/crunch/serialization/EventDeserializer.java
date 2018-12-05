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

package org.pragmaticminds.crunch.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.pragmaticminds.crunch.events.GenericEvent;
import org.pragmaticminds.crunch.events.UntypedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Own deserializer for {@link UntypedEvent}s
 */
public class EventDeserializer implements Deserializer<GenericEvent> {
    private static final Logger logger = LoggerFactory.getLogger(EventDeserializer.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) { /* do nothing */}

    @Override
    public GenericEvent deserialize(String topic, byte[] data) {
        UntypedEvent event = null;
        try {
            event = objectMapper.readValue(data, UntypedEvent.class);
        } catch (IOException e) {
            logger.error("deserialization failed for object", e);
            return null;
        }
        return event.asEvent();
    }

    @Override
    public void close() { /* do nothing */ }

}
