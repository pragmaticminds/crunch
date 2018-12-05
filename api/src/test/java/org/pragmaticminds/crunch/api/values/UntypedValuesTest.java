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

package org.pragmaticminds.crunch.api.values;

import org.junit.Test;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.dates.LongValue;

import java.security.InvalidParameterException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.*;

/**
 * Tests for the UntypedValues.
 *
 * @author julian
 * Created by julian on 03.11.17
 */
public class UntypedValuesTest {

    @Test
    public void getTimestamp_fromInterface_worksAsExpected() {
        MRecord event = UntypedValues.builder().timestamp(13L).build();

        assertEquals(13L, event.getTimestamp());
    }

    @Test
    public void toTypedValues() {
        UntypedValues untypedValues = UntypedValues.builder()
                .timestamp(13L)
                .prefix("")
                .source("test")
                .values(Collections.singletonMap("key", 42L))
                .build();

        TypedValues typedValues = untypedValues.toTypedValues();

        assertEquals(untypedValues.getTimestamp(), typedValues.getTimestamp());
        assertEquals(untypedValues.getSource(), typedValues.getSource());
        assertTrue(LongValue.class.isAssignableFrom(typedValues.getValues().get("key").getClass()));
        assertEquals(42L, untypedValues.getValues().get("key"));
    }

    @Test
    public void toTypedValuesWithNulls() {
        UntypedValues untypedValues = UntypedValues.builder()
                .timestamp(13L)
                .prefix(null)
                .source("test")
                .values(Collections.singletonMap("key", null))
                .build();

        TypedValues typedValues = untypedValues.toTypedValues();

        assertEquals(untypedValues.getTimestamp(), typedValues.getTimestamp());
        assertEquals(untypedValues.getSource(), typedValues.getSource());
        assertNull(typedValues.getValues().get("key"));
    }

    @Test(expected = InvalidParameterException.class)
    public void toTypeValues_fails() {
        UntypedValues untypedValues = UntypedValues.builder()
                .timestamp(13L)
                .source("test")
                .prefix("")
                .values(Collections.singletonMap("key", Instant.now()))
                .build();

        untypedValues.toTypedValues();
    }

    @Test
    public void filterChannelNames_keepOne() {
        UntypedValues untypedValues = getUntypedValues();

        untypedValues = untypedValues.filterChannels(Collections.singleton("channel1"));

        assertEquals(1, untypedValues.getValues().size());
    }

    @Test
    public void filterChannelNames_keepAll() {
        UntypedValues untypedValues = getUntypedValues();

        HashSet<String> names = new HashSet<>();
        names.add("channel1");
        names.add("channel2");
        names.add("channel3");

        untypedValues = untypedValues.filterChannels(names);

        assertEquals(3, untypedValues.getValues().size());
    }

    @Test
    public void filterChannelNames_keepNone_isEmpty() {
        UntypedValues untypedValues = getUntypedValues();

        HashSet<String> names = new HashSet<>();
        names.add("channel4");
        names.add("channel5");
        names.add("channel6");

        untypedValues = untypedValues.filterChannels(names);

        assertEquals(0, untypedValues.getValues().size());
        assertTrue(untypedValues.isEmpty());
    }

    @Test
    public void toStringTest() {
        String string = getUntypedValues().toString();
        assertNotNull(string);
        assertEquals(
                "UntypedValues(source=test, timestamp=13, prefix=, values={channel1=1, channel2=2, channel3=3})",
                string
        );
    }

    @Test
    public void toStringTestWithNulls() {
        UntypedValues values = UntypedValues.builder()
                .values(Collections.singletonMap("key", null))
                .build();
        String string = values.toString();
        assertNotNull(string);
        System.out.println(string);
        assertEquals(
                "UntypedValues(source=null, timestamp=0, prefix=null, values={key=null})",
                string
        );
    }

    private UntypedValues getUntypedValues() {
        HashMap<String, Object> values = new HashMap<>();
        values.put("channel1", 1);
        values.put("channel2", 2);
        values.put("channel3", 3);
        return UntypedValues.builder()
                .timestamp(13L)
                .source("test")
                .prefix("")
                .values(values)
                .build();
    }
}