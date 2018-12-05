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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 03.09.2018
 */
public class JsonDeserializerWrapperTest {

    private JsonDeserializerWrapper jsonDeserializerWrapper;
    private JsonSerializer<String> jsonSerializer;
    private byte[] data;
    private String thisIsATestCall;

    @Before
    public void setUp() {
        jsonDeserializerWrapper = new JsonDeserializerWrapper(String.class);
        jsonSerializer = new JsonSerializer<>();
        thisIsATestCall = "this is a test call";
        data = jsonSerializer.serialize(thisIsATestCall);
    }

    @Test
    public void configure() {
        // does nothing -> nothing should happen
        jsonDeserializerWrapper.configure(null, false);
    }

    @Test
    public void deserialize() {
        String result = (String) jsonDeserializerWrapper.deserialize("topic0815", data);
        assertEquals(thisIsATestCall, result);
    }

    @After
    public void close() {
        jsonDeserializerWrapper.close();
    }
}