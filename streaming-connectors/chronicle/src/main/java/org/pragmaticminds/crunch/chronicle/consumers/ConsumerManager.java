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

package org.pragmaticminds.crunch.chronicle.consumers;

import java.io.Serializable;

/**
 * Stores Offsets and returns them on request
 *
 * @author julian
 * Created by julian on 16.08.18
 */
public interface ConsumerManager extends AutoCloseable, Serializable {

    /**
     * Returns the offset associated with the given consumer
     *
     * @param consumer Name of the consumer
     * @return Offset of the consumer or -1 if no offset is stored
     */
    long getOffset(String consumer);

    /**
     * Acknowledges, i.e., stores the offset persistend
     *
     * @param consumer name of the consumer
     * @param offset   offset to store
     * @param useAcknowledgeRate if true, only acknowledge when count reaches given rate, otherwise always acknowledge
     */
    void acknowledgeOffset(String consumer, long offset, boolean useAcknowledgeRate);
}
