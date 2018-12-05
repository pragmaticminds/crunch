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

package org.pragmaticminds.crunch.runtime.sort;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.pragmaticminds.crunch.events.GenericEvent;

/**
 * An assigner for Watermarks that allows a fixed time of "out of sync" time.
 * The time is given in ms in the constructur.
 * <p>
 * This means that an event which happend before another event has to arrive inside the given timewindow to be recognized.
 * <p>
 * It internally tracks the "maximum" time obsevet yet and emits a watermark that is behint this time with the given delay.
 * <p>
 * The description for this procedure is found in:
 * https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/event_timestamps_watermarks.html#with-periodic-watermarks
 *
 * @author julian
 * Created by julian on 03.11.17
 */
public class EventTimeAssigner implements AssignerWithPunctuatedWatermarks<GenericEvent> {

    // Delay in ms
    private final long delayMs;

    // Maximum time observed yet
    private long currentMaxTimestamp;

    /**
     * Uses the given Delay.
     *
     * @param delayMs Delay for the out of sync messages in ms
     */
    public EventTimeAssigner(long delayMs) {
        this.delayMs = delayMs;
        this.currentMaxTimestamp = Long.MIN_VALUE;
    }

    @Override
    public long extractTimestamp(GenericEvent event, long previousElementTimestamp) {
        long timestamp = event.getTimestamp();
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }

    /**
     * The next watermark is the current extracted timestamp minus the delay.
     *
     * @param event
     * @param extractedTimestamp
     * @return
     */
    @Override
    public Watermark checkAndGetNextWatermark(GenericEvent event, long extractedTimestamp) {
        // simply emit a watermark with every event
        return new Watermark(currentMaxTimestamp - delayMs);
    }
}