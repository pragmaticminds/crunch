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

package org.pragmaticminds.crunch.events;

import org.junit.Test;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.sql.Date;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

/**
 * Tests the functionality of the GenericEventBuilder
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 21.11.2017
 */
public class EventBuilderTest {

    @Test(expected = NullPointerException.class)
    public void withTimestamp() {
        GenericEvent event = GenericEventBuilder.anEvent().withTimestamp(1L).build();
        assertNotNull(event);
    }

    @Test(expected = NullPointerException.class)
    public void withEvent() {
        GenericEvent event = GenericEventBuilder.anEvent().withTimestamp(1L).withEvent("test0815").build();
        assertNotNull(event);
    }

    @Test
    public void withParameters() {
        Map<String, Value> parameters = new HashMap<>();
        parameters.put("test", Value.of("test"));
        GenericEvent event = GenericEventBuilder.anEvent()
                .withTimestamp(1L)
                .withEvent("test0815")
                .withSource("")
                .withParameters(parameters)
                .build();
        assertNotNull(event);
    }

    @Test
    public void withParameterString() {
        GenericEvent event = GenericEventBuilder.anEvent()
                .withTimestamp(1L)
                .withEvent("test0815")
                .withSource("")
                .withParameter("string", "string")
                .build();
        assertNotNull(event);
    }

    @Test
    public void withParameterDouble() {
        GenericEvent event = GenericEventBuilder.anEvent()
                .withTimestamp(1L)
                .withEvent("test0815")
                .withSource("")
                .withParameter("double", 0.1D)
                .build();
        assertNotNull(event);
    }

    @Test
    public void withParameterLong() {
        GenericEvent event = GenericEventBuilder.anEvent()
                .withTimestamp(1L)
                .withEvent("test0815")
                .withSource("")
                .withParameter("long", 1L)
                .build();
        assertNotNull(event);
    }

    @Test
    public void withParameterDate() {
        GenericEvent event = GenericEventBuilder.anEvent()
                .withTimestamp(1L)
                .withEvent("test0815")
                .withSource("")
                .withParameter("date", Date.from(Instant.ofEpochMilli(0L)))
                .build();
        assertNotNull(event);
    }

    @Test
    public void withParameterBoolean() {
        GenericEvent event = GenericEventBuilder.anEvent()
                .withTimestamp(1L)
                .withEvent("test0815")
                .withSource("")
                .withParameter("boolean", false)
                .build();
        assertNotNull(event);
    }

    @Test
    public void withParameterValue() {
        GenericEvent event = GenericEventBuilder.anEvent()
                .withTimestamp(1L)
                .withEvent("test0815")
                .withSource("")
                .withParameter("value", Value.of("value"))
                .build();
        assertNotNull(event);
    }
}