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

package org.pragmaticminds.crunch.api.pipe;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.util.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 03.10.2018
 */
public class ChannelFilterTest {

    ChannelFilter<String> filter;

    @Before
    public void setUp() throws Exception {
        filter = new ChannelFilter<>(
                SubStream.<String>builder()
                        .withPredicate(values -> true)
                        .withIdentifier("test")
                        .withRecordHandler(new MyRecordHandler())
                        .withEvaluationFunction(new MyEvaluationFunction())
                        .build()
        );
    }

    @Test
    public void filter() {
        Map<String, Value> values0 = new HashMap<>();
        values0.put("channel1", Value.of("test"));
        values0.put("channelX", Value.of("test"));

        MRecord record0 = new TypedValues("test", 123L, values0);

        assertTrue(filter.filter(record0));

        Map<String, Value> values1 = new HashMap<>();
        values1.put("channel4", Value.of("test"));
        values1.put("channelX", Value.of("test"));

        MRecord record1 = new TypedValues("test", 123L, values1);

        assertTrue(filter.filter(record1));

        Map<String, Value> values2 = new HashMap<>();
        values2.put("channelX", Value.of("test"));
        values2.put("channelY", Value.of("test"));

        MRecord record2 = new TypedValues("test", 123L, values2);

        assertFalse(filter.filter(record2));
    }

    public static class MyRecordHandler implements RecordHandler {
        @Override public void init() { }

        @Override public void apply(MRecord record) { }

        @Override public void close() { }

        @Override public Set<String> getChannelIdentifiers() {
            return new HashSet<>(Arrays.asList("channel1", "channel2"));
        }
    }

    public static class MyEvaluationFunction implements EvaluationFunction<String> {
        @Override public void eval(EvaluationContext<String> ctx) { }

        @Override public Set<String> getChannelIdentifiers() {
            return new HashSet<>(Arrays.asList("channel2", "channel3", "channel4"));
        }
    }
}