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

import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Test;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.UntypedValues;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test for SortFunction
 *
 * @author julian
 * Created by julian on 03.11.17
 */
public class SortFunctionIT {

    /**
     * Sorts three out of order events.
     *
     * @throws Exception
     */
    @Test
    public void testSorting() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(1);

        // GenericEvent Time Processing
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // results are collected in a static variable
        CollectSink.values.clear();

        // create a stream of custom elements and apply transformations

        UntypedValues event1 = UntypedValues.builder().timestamp(10).build();
        UntypedValues event2 = UntypedValues.builder().timestamp(15).build();
        UntypedValues event3 = UntypedValues.builder().timestamp(20).build();

        env.fromElements(event3, event1, event2)
                .map(untypedValues -> (MRecord) untypedValues)
                .assignTimestampsAndWatermarks(new ValueEventAssigner(15))
                .keyBy(untypedValues -> 1L)
                .process(new SortFunction(50))
                .addSink(new CollectSink());

        // execute
        env.execute();

        // verify your results
        assertEquals(Lists.newArrayList(event1, event2, event3), CollectSink.values);
    }

    /**
     * Sorts three out of order events and discards one of those, because it is too old.
     *
     * @throws Exception
     */
    @Test
    public void testSorting_oldElementIsDiscarded() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(1);

        // GenericEvent Time Processing
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // results are collected in a static variable
        CollectSink.values.clear();

        // create a stream of custom elements and apply transformations

        UntypedValues event1 = UntypedValues.builder().timestamp(100).build();
        UntypedValues event2 = UntypedValues.builder().timestamp(110).build();
        UntypedValues event3 = UntypedValues.builder().timestamp(10).build();

        env.fromElements(event1, event2, event3)
                .map(untypedValues -> (MRecord) untypedValues)
                .assignTimestampsAndWatermarks(new ValueEventAssigner(15))
                .keyBy(untypedValues -> 1L)
                .process(new SortFunction(50))
                .addSink(new CollectSink());

        // execute
        env.execute();

        // verify your results
        assertEquals(Lists.newArrayList(event1, event2), CollectSink.values);
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<MRecord> {

        // must be static
        public static final List<MRecord> values = new ArrayList<>();

        @Override
        public synchronized void invoke(MRecord value) {
            values.add(value);
        }
    }

}