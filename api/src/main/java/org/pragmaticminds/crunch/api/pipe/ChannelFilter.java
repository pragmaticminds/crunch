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

package org.pragmaticminds.crunch.api.pipe;

import org.pragmaticminds.crunch.api.records.MRecord;

import java.io.Serializable;
import java.util.Collection;

/**
 * This class filters incoming {@link MRecord}s by the channel identifiers of all elements inside the {@link SubStream}.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 03.10.2018
 */
public class ChannelFilter<T extends Serializable> implements Serializable {
    private final SubStream<T> subStream;

    /**
     * Main constructor taking the SubStream, the source of all filtering operations.
     *
     * @param subStream containing the channel identifiers.
     */
    public ChannelFilter(SubStream<T> subStream) {
        this.subStream = subStream;
    }

    /**
     * Filters incoming records. Passes only those which have at least on of the processed channel identifiers from
     * the SubStream.
     *
     * @param record to be filtered out or not
     * @return true if record can pass trough else false
     */
    public boolean filter(MRecord record){
        Collection<String> recordChannels = record.getChannels();
        Collection<String> subStreamChannels = subStream.getChannelIdentifiers();
        for (String recordChannel : recordChannels){
            if(subStreamChannels.contains(recordChannel)){
                return true;
            }
        }
        return false;
    }
}
