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

import java.util.HashMap;
import java.util.Map;

/**
 * {@link ConsumerManager} which uses an in memory map.
 * Useful for Testing.
 *
 * @author julian
 * Created by julian on 16.08.18
 */
public class MemoryManager implements ConsumerManager {

    private Map<String, Long> offsetMap = new HashMap<>();

    @Override
    public long getOffset(String consumer) {
        if (offsetMap.containsKey(consumer)) {
            return offsetMap.get(consumer);
        } else {
            return -1;
        }
    }

    @Override
    public void acknowledgeOffset(String consumer, long offset, boolean useAcknowledgeRate) {
        offsetMap.put(consumer, offset);
    }


    @Override
    public void close() {
        // do nothing here
    }
}
