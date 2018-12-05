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

package org.pragmaticminds.crunch.api.records;

import org.pragmaticminds.crunch.api.exceptions.RecordItemConversionException;
import org.pragmaticminds.crunch.api.exceptions.UnknownRecordItemException;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.util.Collection;
import java.util.Date;

/**
 * Interface for Machine Records.
 * Machine Records are basically characterized by the following
 * <ul>
 *     <li>They have a timestamp</li>
 *     <li>They have a source identifier</li>
 *     <li>They have not only one value for this timestamp but several (possibly many)</li>
 * </ul>
 *
 * Thus, this interface consists of getters for
 * <ul>
 *     <li>timestamp</li>
 *     <li>source</li>
 *     <li>the contained items</li>
 * </ul>
 *
 * Several typesafe getter methods are provided.
 * If a channel does not exist they have to throw a {@link UnknownRecordItemException} and if the value cannot be provided
 * in the expected type a {@link RecordItemConversionException} should be thrown.
 *
 * @author julian
 * Created by julian on 03.11.17
 */
public interface MRecord extends CRecord {

    /**
     * Getter
     *
     * @return Source Identifier
     */
    String getSource();

    /**
     * Typed Nullable Getter
     *
     * @param channel Channel to extract
     * @return Channel Value as double
     */
    Double getDouble(String channel);

    /**
     * Typed Nullable Getter
     *
     * @param channel Channel to extract
     * @return Channel value as Long
     */
    Long getLong(String channel);

    /**
     * Typed Nullable Getter
     *
     * @param channel Channel to extract
     * @return Channel Value as Boolean
     */
    Boolean getBoolean(String channel);

    /**
     * Typed Nullable Getter
     *
     * @param channel Channel to extract
     * @return Channel Value as Date
     */
    Date getDate(String channel);

    /**
     * Typed Nullable Getter
     *
     * @param channel Channel to extract
     * @return Channel Value as String
     */
    String getString(String channel);

    /**
     * Getter
     *
     * @param channel Channel to Extract
     * @return Channel Value as {@link Value}
     */
    Value getValue(String channel);

    /**
     * Untyped Getter, use with care!
     *
     * @param channel Channel to extract
     * @return Channel Value as {@link Object}
     */
    Object get(String channel);

    /**
     * Return a Collection of all channels in this Record.
     * @return Collection with all channels.
     */
    Collection<String> getChannels();

}
