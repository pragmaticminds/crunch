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

package org.pragmaticminds.crunch.execution;

import java.io.Serializable;
import java.util.Collection;

/**
 * This interface describes the structure for a timestamp based sort function.
 * All incoming values are processed by the {@link #process(Serializable, Serializable, Serializable)} function and
 * stored into an internal queue. A Timer is than calling the {@link #onTimer(Serializable)} method to collect all
 * values that are over the given watermark timestamp.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 02.10.2018
 */
public interface SortFunction<I extends Serializable, T extends Serializable> extends Serializable {

    /**
     * Takes a value into the internal queue.
     *
     * @param timestamp relevant timestamp for sorting.
     * @param watermark current watermark timestamp as the top limiter.
     * @param value to be sorted by timestamp.
     */
    void process(I timestamp, I watermark, T value);

    /**
     * This method is called by a timer, that periodically collects results from the internal queue, that are over the
     * watermark timestamp and are ready to be processed.
     *
     * @param watermark timestamp, that indicates which queued values are ready to be processed in the next step.
     * @return a {@link Collection} of all values with a timestamp over the given watermark timestamp.
     */
    Collection<T> onTimer(I watermark);
}
