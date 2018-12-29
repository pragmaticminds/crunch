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

package org.pragmaticminds.crunch.api.state;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.events.GenericEvent;

import java.util.Map;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 07.08.2018
 */
public class StateEvaluationContextTest {

    private StateEvaluationContext stateEvaluationContext;
    private TypedValues mockValues;
    private GenericEvent event = new GenericEvent();

    @Before
    public void setUp() throws Exception {
        mockValues = Mockito.mock(TypedValues.class);
        stateEvaluationContext = new StateEvaluationContext(mockValues, "alias") {};
    }

    @Test
    public void fromTypedValues() {
        Assert.assertNotNull(stateEvaluationContext);
    }

    @Test
    public void getEvents() {
        stateEvaluationContext.collect("test", event);
        Map<String, GenericEvent> events = stateEvaluationContext.getEvents();
        Assert.assertNotNull(events);
        Assert.assertNotEquals(0, events.size());
        Assert.assertEquals(event, events.get("test"));
    }

    @Test
    public void get() {
        MRecord typedValues = stateEvaluationContext.get();
        Assert.assertEquals(mockValues, typedValues);
    }

    @Test
    public void collect() {
        // no exception is expected
        stateEvaluationContext.collect("test", event);
    }
}