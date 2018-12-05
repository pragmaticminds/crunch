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

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.values.dates.Value;

import javax.management.openmbean.InvalidKeyException;
import java.sql.Date;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests the conversions from and to {@link UntypedEvent} and {@link GenericEvent}
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 17.11.2017
 */
public class UntypedEventTest {

    private final Value testString = Value.of("testString");
    private final Value testDouble = Value.of((Double) 0.1);
    private final Value testLong = Value.of(1L);
    private final Value testDate = Value.of(Date.from(Instant.now()));
    private final Value testBoolean = Value.of(true);
    private GenericEvent event;
    private Map<String, Value> parameters;

    @Before
    public void setUp() {
        parameters = new HashMap<>();
        parameters.put("string", testString);
        parameters.put("double", testDouble);
        parameters.put("long", testLong);
        parameters.put("date", testDate);
        parameters.put("boolean", testBoolean);
        event = new GenericEvent(1L, "testEvent", "testSource", parameters);
    }

    @Test
    public void fromEventAndAsEvent() {
        UntypedEvent untypedEvent = UntypedEvent.fromEvent(event);

        GenericEvent resultEvent = untypedEvent.asEvent();

        assertEquals(event, resultEvent);
    }

    @Test
    public void getParameter() {
        UntypedEvent untypedEvent = UntypedEvent.fromEvent(event);

        assertNotNull(untypedEvent.getParameter("string"));
        assertNotNull(untypedEvent.getParameter("double"));
        assertNotNull(untypedEvent.getParameter("long"));
        assertNotNull(untypedEvent.getParameter("date"));
        assertNotNull(untypedEvent.getParameter("boolean"));

        assertNotNull(event.getParameter("string"));
        assertNotNull(event.getParameter("double"));
        assertNotNull(event.getParameter("long"));
        assertNotNull(event.getParameter("date"));
        assertNotNull(event.getParameter("boolean"));
    }

    @Test(expected = InvalidKeyException.class)
    public void getParameterNotInParameters() {
        event.getParameter("notPresent");
    }

    @Test(expected = InvalidKeyException.class)
    public void getParameterFromUntypedNotInParameters() {
        UntypedEvent untypedEvent = UntypedEvent.fromEvent(event);
        untypedEvent.getParameter("notPresent");
    }

}