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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.execution.TimestampSortFunction;

import java.io.IOException;
import java.util.Collection;
import java.util.PriorityQueue;

/**
 * Flink ProcessFunction that Sorts the incoming events.
 * The idea is taken from https://github.com/dataArtisans/flink-training-exercises/blob/master/src/main/java/com/dataartisans/flinktraining/exercises/datastream_java/process/CarEventSort.java.
 *
 * @author julian
 * Created by julian on 03.11.17
 */
public class SortFunction extends ProcessFunction<MRecord, MRecord> {
    private int capacity = 100;
    private transient ValueState<TimestampSortFunction<MRecord>> valueState;

    /** Basic constructor */
    public SortFunction() {
        /* do nothing */
    }

    /**
     * Constructor with the initial capacity of the queue in the {@link #valueState}.
     *
     * @param capacity initial value of the queue in the {@link #valueState}.
     */
    public SortFunction(int capacity) {
        this.capacity = capacity;
    }

    /**
     * Init a ValueStateDescriptor for the Flink Keyed State.
     *
     * @param config a {@link Configuration}
     */
    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<TimestampSortFunction<MRecord>> descriptor = new ValueStateDescriptor<>(
                // state name
                "sorted-events",
                // type information of state
                TypeInformation.of(new TypeHint<TimestampSortFunction<MRecord>>() {})
        );
        valueState = getRuntimeContext().getState(descriptor);
    }

    /**
     * The Element is added to a {@link PriorityQueue} and a timer is set to check on time.
     * If the element is "too old", this means below the current watermark, it is discarded and a warning is logged.
     *
     * @param record of {@link MRecord} type.
     * @param context of type {@link Context} from the {@link ProcessFunction}.
     * @param out of {@link Collector} type, collects the resulting {@link MRecord}s.
     */
    @Override
    public void processElement(MRecord record, Context context, Collector<MRecord> out) throws IOException {
        TimerService timerService = context.timerService();

        TimestampSortFunction<MRecord> innerSortFunction = valueState.value();
        if(innerSortFunction == null){
            innerSortFunction = new TimestampSortFunction<>(capacity);
        }

        // process record
        innerSortFunction.process(
                record.getTimestamp(),
                timerService.currentWatermark(),
                record
        );

        // set timer
        timerService.registerEventTimeTimer(record.getTimestamp());

        valueState.update(innerSortFunction);
    }

    /**
     * Checks if there are events that are "old enough" to be emitted (below the current watermark) and emits them, if so.
     *
     * @param timestamp ignored value
     * @param context   delivers the {@link TimerService} for the calling of the
     *                  {@link #valueState#onTimer(Long)} method.
     * @param out       Takes the {@link MRecord}s that are over the watermark timestamp from the
     *                  context#timerService#currentWatermark.
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<MRecord> out) throws IOException {
        Long watermark = context.timerService().currentWatermark();

        TimestampSortFunction<MRecord> innerSortFunction = valueState.value();
        if(innerSortFunction == null){
            innerSortFunction = new TimestampSortFunction<>(capacity);
        }

        Collection<MRecord> results = innerSortFunction.onTimer(watermark);
        for (MRecord record : results){
            out.collect(record);
        }

        valueState.update(innerSortFunction);
    }
}

