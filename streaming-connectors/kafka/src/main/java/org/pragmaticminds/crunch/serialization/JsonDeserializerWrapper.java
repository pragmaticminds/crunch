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

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * This class wraps a {@link org.pragmaticminds.crunch.serialization.Deserializer} into a {@link Deserializer}, so that
 * it appears as the other kind.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 03.09.2018
 */
public class JsonDeserializerWrapper<T> implements Deserializer<T> {
    private final JsonDeserializer<T> innerDeserializer;

    /**
     * Main constructor, which takes the type class to deserialize
     * @param clazz to be deserialized
     */
    public JsonDeserializerWrapper(Class<T> clazz) {
        this.innerDeserializer = new JsonDeserializer<>(clazz);
    }

    /**
     * Configure this class.
     *
     * @param configs configs in key/value pairs
     * @param isKey   whether is for key or value
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        /* nothing to configure */
    }

    /**
     * Deserialize a record value from a byte array into a value or object.
     *
     * @param topic topic associated with the data
     * @param data  serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    @Override
    @SuppressWarnings("unchecked") // user must make sure the deserialization is to the right type
    public T deserialize(String topic, byte[] data) {
        return innerDeserializer.deserialize(data);
    }

    /**
     * pass close call to the inner deserializer
     */
    @Override
    public void close() {
        innerDeserializer.close();
    }
}
