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

package org.pragmaticminds.crunch.execution;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;
import org.pragmaticminds.crunch.api.values.UntypedValues;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 03.10.2018
 */
public class UntypedValuesMergeFunctionTest implements Serializable {
    UntypedValuesMergeFunction mergeFunction;
    UntypedValuesMergeFunction clone;
    UntypedValues record1;
    UntypedValues record2;

    @Before
    public void setUp() throws Exception {
        mergeFunction = new UntypedValuesMergeFunction();
        clone = ClonerUtil.clone(mergeFunction);
        Map<String, Object> values1 = new HashMap<>();
        values1.put("test1", 123L);
        Map<String, Object> values2 = new HashMap<>();
        values2.put("test2", "string");
        record1 = new UntypedValues("test", 123L, "prefix", values1);
        record2 = new UntypedValues("test", 124L, "prefix", values2);
    }

    @Test
    public void merge() {
        mergeFunction.merge(record1);
        UntypedValues result = (UntypedValues) mergeFunction.merge(record2);
        assertEquals(123L, (long)result.getValue("test1").getAsLong());
        assertEquals("string", result.getValue("test2").getAsString());

        clone.merge(record1);
        result = (UntypedValues) clone.merge(record2);
        assertEquals(123L, (long)result.getValue("test1").getAsLong());
        assertEquals("string", result.getValue("test2").getAsString());
    }

    @Test
    public void mapWithoutState() {
        UntypedValues result = mergeFunction.mapWithoutState(record1, record2);
        assertEquals(123L, (long)result.getValue("test1").getAsLong());
        assertEquals("string", result.getValue("test2").getAsString());

        result = clone.mapWithoutState(record1, record2);
        assertEquals(123L, (long)result.getValue("test1").getAsLong());
        assertEquals("string", result.getValue("test2").getAsString());
    }
}