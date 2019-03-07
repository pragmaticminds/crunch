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

package org.pragmaticminds.crunch.api.windowed.filter;

import org.pragmaticminds.crunch.api.records.MRecord;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * Filters Ts by itself and the {@link MRecord} {@link List} that was the base for the creation
 * of the T.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public interface EventAndRecordsFilter<T extends Serializable> extends Serializable {

    /**
     * Filters all Events that should not be passed to further processing.
     * The filtering is based on the T itself and on the {@link MRecord} {@link List} to
     * determine if an T is to be passed further.
     *
     * @param event of interest, that should be filtered or not.
     * @param records that were used to create the T.
     * @return true if the T is to be send further for processing, otherwise false.
     */
    boolean apply(T event, Collection<MRecord> records);

    /**
     * Collects all channel identifiers that are used in this filter.
     *
     * @return a {@link List} or {@link Collection} with all channels that are used to filter.
     */
    Collection<String> getChannelIdentifiers();
}
