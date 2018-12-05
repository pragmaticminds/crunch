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

import java.io.Serializable;

/**
 * An interface for converting bytes into object.
 *
 * @param <T> Type to be deserialized to
 * @author julian
 */
public interface Deserializer<T> extends AutoCloseable, Serializable {

    /**
     * Convert {@code data} into a byte array.
     *
     * @param bytes serilaized bytes
     * @return typed object
     */
    T deserialize(byte[] bytes);

    /**
     * Close this serializer.
     * <p>
     * This method must be idempotent as it may be called multiple times.
     */
    @Override
    void close();
}
