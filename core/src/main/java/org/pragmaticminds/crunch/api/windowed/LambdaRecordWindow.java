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

package org.pragmaticminds.crunch.api.windowed;

import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.comparator.SerializableFunction;
import org.pragmaticminds.crunch.api.trigger.comparator.SerializableResultFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Wraps the {@link RecordWindow} interface so that it can be implemented with lambdas.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.09.2018
 */
public class LambdaRecordWindow implements RecordWindow {

    private SerializableFunction<MRecord, Boolean> inWindowLambda;
    private SerializableResultFunction<ArrayList<String>> getChannelIdentifiers;

    public LambdaRecordWindow(
            SerializableFunction<MRecord, Boolean> inWindowLambda,
            SerializableResultFunction<ArrayList<String>> getChannelIdentifiers
    ) {
        this.inWindowLambda = inWindowLambda;
        this.getChannelIdentifiers = getChannelIdentifiers;
    }

    /**
     * Checks if a processing window is open or closed.
     * In case of open -> all records will be accumulated in the next processing instance of the record.
     * In case of closed -> all records and the current are dropped in the next processing instance of the record.
     *
     * @param record is checked if a window is open or closed
     * @return true if window is open, false otherwise
     */
    @Override
    public boolean inWindow(MRecord record) {
        return inWindowLambda.apply(record);
    }

    /**
     * Collects all identifiers of channels that are used in here.
     *
     * @return a {@link List} or {@link Collection} of channel identifiers.
     */
    @Override
    public Collection<String> getChannelIdentifiers() {
        return getChannelIdentifiers.get();
    }
}
