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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.pragmaticminds.crunch.events.GenericEvent;
import org.pragmaticminds.crunch.events.UntypedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Own serializer for {@link UntypedEvent}s
 */
public class EventSerializer implements Serializer<GenericEvent> {

    private static final Logger logger = LoggerFactory.getLogger(EventSerializer.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) { /* do nothing */}

    @Override
    public byte[] serialize(String topic, GenericEvent event) {
        try {
            UntypedEvent untypedEvent = UntypedEvent.fromEvent(event);
            return objectMapper.writeValueAsBytes(untypedEvent);
        } catch (JsonProcessingException e) {
            logger.error("could not deserialize GenericEvent", e);
            return new byte[0];
        }
    }

    @Override
    public void close() { /* do nothing */ }
}
