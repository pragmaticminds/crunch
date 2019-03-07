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

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * This Interface checks if a record window is opened for accumulation of records or if it is closed to drop all
 * accumulated records and the current record.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public interface RecordWindow extends Serializable {
    /**
     * Checks if a processing window is open or closed.
     * In case of open -> all records will be accumulated in the next processing instance of the record.
     * In case of closed -> all records and the current are dropped in the next processing instance of the record.
     * @param record is checked if a window is open or closed
     * @return true if window is open, false otherwise
     */
    boolean inWindow(MRecord record);

    /**
     * Collects all identifiers of channels that are used in here.
     *
     * @return a {@link List} or {@link Collection} of channel identifiers.
     */
    Collection<String> getChannelIdentifiers();
}
