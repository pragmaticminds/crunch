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

package org.pragmaticminds.crunch.sinks;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.junit.Test;
import org.mockito.Mockito;
import org.pragmaticminds.crunch.api.pipe.EvaluationContext;
import org.pragmaticminds.crunch.api.pipe.RecordHandler;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.GenericEvent;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 *
 * @author julian
 * Created by julian on 15.08.18
 */
public class InfluxDBSinkTest {

    @Test
    public void callEval() {
        // Mock factory
        InfluxDB mock = Mockito.mock(InfluxDB.class);
        InfluxDBSink.InfluxFactory factory = () -> mock;

        // Create the EvalFunction
        RecordHandler sink = new InfluxDBSink(factory, "meas1");

        // Perform
        sink.init();
        sink.apply(createUntypedValues());
        sink.apply(createTypedValues());
        sink.close();

        // Verify that something has been done
        verify(mock, times(6)).write(any(Point.class));
        assertTrue(sink.getChannelIdentifiers().isEmpty());
    }

    private UntypedValues createUntypedValues() {
        HashMap<String, Object> values = new HashMap<>();
        values.put("value1", 1L);
        values.put("value2", "s");
        values.put("value3", 3.0);
        values.put("value4", true);
        values.put("value5", new Date(Instant.now().toEpochMilli()));
        return UntypedValues.builder()
                .source("source")
                .prefix("")
                .timestamp(0L)
                .values(values)
                .build();
    }

    private TypedValues createTypedValues() {
        return TypedValues.builder()
                .source("source")
                .timestamp(0L)
                .values(Collections.singletonMap("value", Value.of("String")))
                .build();
    }

    private EvaluationContext createContext(MRecord record) {
        return new EvaluationContext<GenericEvent>() {
            @Override
            public MRecord get() {
                return record;
            }

            @Override
            public void collect(GenericEvent event) {

            }
        };
    }

    @Test
    public void testHistory() {
        Map<String, InfluxDBSink.HistoryObject> historyObjectMap = new HashMap<>();
        String varName = "testVar";
        String varName2 = "testVar2";
        Value value = Value.of(123.45);
        long timestamp = 123456L;

        assertTrue(historyObjectMap.isEmpty());
        InfluxDBSink.checkHistoryAndInsertIfNeeded(historyObjectMap,varName,value,timestamp);
        assertFalse(historyObjectMap.isEmpty());
        assertEquals(historyObjectMap.get(varName).getValue(),value);
        assertEquals(historyObjectMap.get(varName).getTime(),timestamp);

        //new value and greater time --> should change map entry
        value = Value.of(123.46);
        timestamp = 123457L;
        InfluxDBSink.checkHistoryAndInsertIfNeeded(historyObjectMap,varName,value,timestamp);
        assertFalse(historyObjectMap.isEmpty());
        assertEquals(historyObjectMap.get(varName).getValue(),value);
        assertEquals(historyObjectMap.get(varName).getTime(),timestamp);

        //new value and lower time --> no map change excepted
        value = Value.of(123.47);
        timestamp = 123456L;
        InfluxDBSink.checkHistoryAndInsertIfNeeded(historyObjectMap,varName,value,timestamp);
        assertFalse(historyObjectMap.isEmpty());
        assertNotEquals(historyObjectMap.get(varName).getValue(),value);
        assertNotEquals(historyObjectMap.get(varName).getTime(),timestamp);

        //last value and new time lower then offset-limit (10s) --> no map change expected
        value = Value.of(123.46);
        timestamp = 124457L;
        InfluxDBSink.checkHistoryAndInsertIfNeeded(historyObjectMap,varName,value,timestamp);
        assertFalse(historyObjectMap.isEmpty());
        assertEquals(historyObjectMap.get(varName).getValue(),value);
        assertNotEquals(historyObjectMap.get(varName).getTime(),timestamp);

        //last value and greater time then limit-delta (10s) --> should change map entry
        value = Value.of(123.47);
        timestamp = 134456L;
        InfluxDBSink.checkHistoryAndInsertIfNeeded(historyObjectMap,varName,value,timestamp);
        assertFalse(historyObjectMap.isEmpty());
        assertEquals(historyObjectMap.get(varName).getValue(),value);
        assertEquals(historyObjectMap.get(varName).getTime(),timestamp);

        //second variable
        value = Value.of(123.47);
        timestamp = 134456L;
        InfluxDBSink.checkHistoryAndInsertIfNeeded(historyObjectMap,varName2,value,timestamp);
        assertEquals(2,historyObjectMap.size());
        assertEquals(historyObjectMap.get(varName2).getValue(),value);
        assertEquals(historyObjectMap.get(varName2).getTime(),timestamp);

    }
}
