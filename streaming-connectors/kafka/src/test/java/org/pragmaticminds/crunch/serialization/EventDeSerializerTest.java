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

package org.pragmaticminds.crunch.serialization;

import org.junit.Assert;
import org.junit.Test;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.GenericEvent;
import org.pragmaticminds.crunch.events.GenericEventBuilder;

/**
 *
 *
 * @author julian
 * Created by julian on 14.08.18
 */
public class EventDeSerializerTest {

    @Test
    public void deSerializeEvent() {
        GenericEvent event = GenericEventBuilder.anEvent()
                .withEvent("Type")
                .withTimestamp(1L)
                .withSource("me")
                .withParameter("a", Value.of(1L))
                .withParameter("b", Value.of("String"))
                .build();

        GenericEvent event1;
        try (EventSerializer ser = new EventSerializer()) {
            try (EventDeserializer deSer = new EventDeserializer()) {

                byte[] bytes = ser.serialize("", event);
                event1 = deSer.deserialize("", bytes);
            }
        }

        Assert.assertEquals(event, event1);
    }
}