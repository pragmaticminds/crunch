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

package org.pragmaticminds.crunch.api.trigger.filter;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.GenericEvent;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.stringChannel;
import static org.pragmaticminds.crunch.api.trigger.filter.EventFilters.onValueChanged;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 14.08.2018
 */
public class EventFiltersTest {

    private TypedValues values1;
    private TypedValues values2;
    private TypedValues valuesNull;

    @Before
    public void setUp() throws Exception {
        Map<String, Value> valueMap1 = new HashMap<>();
        valueMap1.put("val", Value.of("string1"));
        values1 = TypedValues.builder().source("test").timestamp(System.currentTimeMillis()).values(valueMap1).build();

        Map<String, Value> valueMap2 = new HashMap<>();
        valueMap2.put("val", Value.of("string2"));
        values2 = TypedValues.builder().source("test").timestamp(System.currentTimeMillis()).values(valueMap2).build();

        Map<String, Value> valueMap3 = new HashMap<>();
        valuesNull = TypedValues.builder().source("test").timestamp(System.currentTimeMillis()).values(valueMap3).build();
    }

    @Test
    public void valueChangedTest() {
        // value test
        EventFilter<GenericEvent> filter = onValueChanged(stringChannel("val"));

        // first value receive -> false
        assertFalse(filter.apply(new GenericEvent(), values1));
        // first change -> true
        assertTrue(filter.apply(new GenericEvent(), values2));
        // value is null -> false
        assertFalse(filter.apply(new GenericEvent(), valuesNull));
        // value change -> true
        assertTrue(filter.apply(null, values1));
        // no change -> false
        assertFalse(filter.apply(null, values1));
        // value is null again -> false
        assertFalse(filter.apply(null, valuesNull));

        // check get ChannelIdentifiers
        assertTrue(filter.getChannelIdentifiers().contains("val"));

        // serializable test
        EventFilter<GenericEvent> clone = ClonerUtil.clone(filter);
        assertFalse(clone.apply(null, values1));
    }
}
