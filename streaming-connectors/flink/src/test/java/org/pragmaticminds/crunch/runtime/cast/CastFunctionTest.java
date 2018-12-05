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

package org.pragmaticminds.crunch.runtime.cast;

import org.junit.Test;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.UntypedValues;

import java.sql.Date;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author kerstin
 * Created by kerstin on 03.11.17.
 */
public class CastFunctionTest {

    private CastFunction castFunction = new CastFunction();

    @Test
    public void map() {
        long timestamp = Instant.now().toEpochMilli();
        Map<String, Object> map = new HashMap<>();
        map.put("0", Boolean.FALSE);
        map.put("1", Date.from(Instant.ofEpochMilli(timestamp)));
        map.put("2", 2.0);
        map.put("3", 3L);
        map.put("4", "4");

        UntypedValues untypedValues = new UntypedValues("source", timestamp, "testPlc", map);
        TypedValues typedValues = castFunction.map(untypedValues);

        assertNotNull(typedValues);
        assertEquals(untypedValues.getSource(), typedValues.getSource());
        assertEquals(untypedValues.getTimestamp(), typedValues.getTimestamp());
    }

}