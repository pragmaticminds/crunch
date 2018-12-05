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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serializes Objects to JSON
 *
 * @author julian
 * Created by julian on 16.08.18
 */
public class JsonSerializer<T> implements Serializer<T> {

    private static final Logger logger = LoggerFactory.getLogger(JsonSerializer.class);

    private final ObjectMapper mapper;

    public JsonSerializer() {
        mapper = new ObjectMapper(new JsonFactory());
    }

    @Override
    public byte[] serialize(T data) {
        try {
            return mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            logger.warn("Unable to serialize the given object " + data, e);
            return new byte[0];
        }
    }

    @Override
    public void close() {
        // Do nothing
    }
}
