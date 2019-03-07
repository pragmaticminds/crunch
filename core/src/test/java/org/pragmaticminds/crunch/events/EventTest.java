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

import static org.junit.Assert.assertEquals;

/**
 * - Create an GenericEvent as Example
 *
 * @author julian
 * Created by julian on 12.11.17
 */
public class EventTest {

    @Test
    public void builder_createEvent() {
        GenericEvent event = createEvent();

        assertEquals("LHL1", event.getSource());
    }

    private GenericEvent createEvent() {
        return GenericEventBuilder.anEvent()
                .withTimestamp(1L)
                .withSource("LHL1")
                .withEvent("EVENT_NAME")
                .build();
    }

    @Test
    public void copyConstructor() {
        GenericEvent event = createEvent();
        GenericEvent event1 = new GenericEvent(event);
        assertEquals(event, event1);
    }
}