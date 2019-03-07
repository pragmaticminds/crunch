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

import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;

import java.util.ArrayList;

/**
 * This is a collection of {@link RecordWindow} implementations for usual use cases
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public class Windows {
    private Windows() { /* hide constructor */ }

    /**
     * Window is open as long a supplied {@link Boolean} value is true.
     *
     * @param supplier delivers a {@link Boolean} value.
     * @return a RecordWindow that determines if a window is open.
     */
    public static RecordWindow bitActive(Supplier<Boolean> supplier) {
        return new LambdaRecordWindow(
                values -> supplier.extract(values) != null && supplier.extract(values),
                () -> new ArrayList<>(supplier.getChannelIdentifiers())
        );
    }


    /**
     * Window is open as long a supplied {@link Boolean} value is false.
     *
     * @param supplier delivers a {@link Boolean} value.
     * @return a RecordWindow that determines if a window is open.
     */
    public static RecordWindow bitNotActive(Supplier<Boolean> supplier) {
        return new LambdaRecordWindow(
                record -> supplier.extract(record) != null && !supplier.extract(record),
                () -> new ArrayList<>(supplier.getChannelIdentifiers())
        );
    }

    /**
     * Window is open as long a supplied value has the expected value.
     *
     * @param supplier delivers a value.
     * @return a RecordWindow that determines if a window is open.
     */
    public static <T> RecordWindow valueEquals(Supplier<T> supplier, T expected) {
        return new LambdaRecordWindow(
                record -> supplier.extract(record) != null && supplier.extract(record).equals(expected),
                () -> new ArrayList<>(supplier.getChannelIdentifiers())
        );
    }

    /**
     * Window is open as long a supplied value has not the notExpected value.
     *
     * @param supplier delivers a value.
     * @return a RecordWindow that determines if a window is open.
     */
    public static <T> RecordWindow valueNotEquals(Supplier<T> supplier, T notExpected) {
        return new LambdaRecordWindow(
                record -> supplier.extract(record) != null && !supplier.extract(record).equals(notExpected),
                () -> new ArrayList<>(supplier.getChannelIdentifiers())
        );
    }
}