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

import org.pragmaticminds.crunch.api.trigger.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * This class implements the {@link SortFunction} interface based on {@link Long} type timestamps.
 * All incoming values are processed by the {@link #process(Long, Long, Serializable)} function and stored into an internal queue.
 * A Timer is than calling the {@link #onTimer(Long)} method to collect all values that are over the given watermark
 * timestamp.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 02.10.2018
 */
public class TimestampSortFunction<T extends Serializable> implements SortFunction<Long, T> {
    private static final Logger logger = LoggerFactory.getLogger(TimestampSortFunction.class);
    private static final int DEFAULT_CAPACITY = 100;

    private PriorityQueue<Tuple2<Long, T>> queue;

    /** basic main constructor */
    public TimestampSortFunction() {
        queue = new PriorityQueue<>(DEFAULT_CAPACITY, new ValueEventComparator<>());
    }

    /**
     * Constructor that takes the initial capacity ot the internal queue.
     *
     * @param capacity the initial capacity of the internal queue.
     */
    public TimestampSortFunction(int capacity) {
        queue = new PriorityQueue<>(capacity, new ValueEventComparator<>());
    }

    /**
     * Takes a value into the internal queue, if they are not older than the peek value of the queue, otherwise
     * the value is discarded.
     *
     * @param timestamp relevant timestamp for sorting.
     * @param watermark relevant watermark timestamp as the top limit for adding new values to the queue.
     * @param value to be sorted by timestamp.
     */
    @Override
    public void process(Long timestamp, Long watermark, T value) {
        if (queue.peek() == null || timestamp > watermark) {
            queue.add(new Tuple2<>(timestamp,value));
        } else {
            logger.warn("Value with old timestamp is discarded {} for {}", timestamp, value);
        }
    }

    /**
     * This method is called by a timer, that periodically collects results from the internal queue, that are over the
     * watermark timestamp and are ready to be processed.
     *
     * @param watermark timestamp, that indicates which queued values are ready to be processed in the next step,
     *                  which are all values with the timestamp <= watermark timestamp.
     * @return          a {@link Collection} of all values with a timestamp over the given watermark timestamp.
     */
    @Override
    public Collection<T> onTimer(Long watermark) {
        List<T> results = new ArrayList<>();
        Tuple2<Long,T> head = queue.peek();
        while (head != null && head.getF0() <= watermark) {
            results.add(head.getF1());
            queue.remove(head);
            head = queue.peek();
        }
        return results;
    }

    /**
     * Comparator that compares the Long values of the two Timestamps.
     */
    private static class ValueEventComparator<T extends Serializable> implements Comparator<Tuple2<Long, T>>, Serializable {

        /**
         * Use Long compare on the timestamps of both events.
         * @see Long#compare(long, long)
         *
         * @param o1 first operand
         * @param o2 second operand
         * @return an {@link Integer} value representing comparison result
         */
        @Override
        public int compare(Tuple2<Long, T> o1, Tuple2<Long, T> o2) {
            return Long.compare(o1.getF0(), o2.getF0());
        }
    }
}
