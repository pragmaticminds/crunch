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

package org.pragmaticminds.crunch.api.windowed;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.booleanChannel;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public class WindowsTest implements Serializable {

    private TypedValues valuesTrue;
    private TypedValues valuesFalse;
    private TypedValues valuesNull;

    @Before
    public void setUp() throws Exception {
        TypedValues.TypedValuesBuilder valuesBuilder = TypedValues.builder()
                .timestamp(System.currentTimeMillis())
                .source("test");

        Map<String, Value> valueMap1 = new HashMap<>();
        valueMap1.put("flag", Value.of(true));
        valuesTrue = valuesBuilder.values(valueMap1).build();

        Map<String, Value> valueMap2 = new HashMap<>();
        valueMap2.put("flag", Value.of(false));
        valuesFalse = valuesBuilder.values(valueMap2).build();

        Map<String, Value> valueMap3 = new HashMap<>();
        // do not add "flag"
        valuesNull = valuesBuilder.values(valueMap3).build();
    }

    @Test
    public void testBitActive() {
        RecordWindow recordWindow = Windows.bitActive(
                new InnerBooleanSupplier()
        );
        RecordWindow clone = ClonerUtil.clone(recordWindow);

        innerTestBitActiver(recordWindow);
        innerTestBitActiver(clone);
    }

    private void innerTestBitActiver(RecordWindow recordWindow) {
        boolean result = recordWindow.inWindow(valuesTrue);
        assertTrue(result);

        result = recordWindow.inWindow(valuesFalse);
        assertFalse(result);

        result = recordWindow.inWindow(valuesNull);
        assertFalse(result);

        assertTrue(recordWindow.getChannelIdentifiers().contains("flag"));
    }

    @Test
    public void testBitNotActive() {
        RecordWindow recordWindow = Windows.bitNotActive(booleanChannel("flag"));
        RecordWindow clone = ClonerUtil.clone(recordWindow);

        innerTestBitNotActive(recordWindow);
        innerTestBitNotActive(clone);
    }

    private void innerTestBitNotActive(RecordWindow recordWindow) {
        boolean result = recordWindow.inWindow(valuesTrue);
        assertFalse(result);

        result = recordWindow.inWindow(valuesFalse);
        assertTrue(result);

        result = recordWindow.inWindow(valuesNull);
        assertFalse(result);

        assertTrue(recordWindow.getChannelIdentifiers().contains("flag"));
    }

    @Test
    public void testValueEquals() {
        RecordWindow recordWindow = Windows.valueEquals(booleanChannel("flag"), true);
        RecordWindow clone = ClonerUtil.clone(recordWindow);

        innerTestBitActiver(recordWindow);
        innerTestBitActiver(clone);
    }

    @Test
    public void testValueNotEquals() {
        RecordWindow recordWindow = Windows.valueNotEquals(booleanChannel("flag"), true);
        RecordWindow clone = ClonerUtil.clone(recordWindow);

        innerTestBitNotActive(recordWindow);
        innerTestBitNotActive(clone);
    }

    private class InnerBooleanSupplier implements Supplier<Boolean> {
        @Override
        public Boolean extract(MRecord values) {
            return values.getBoolean("flag");
        }

        @Override
        public String getIdentifier() {
            return "flag";
        }

        @Override
        public Set<String> getChannelIdentifiers() {
            return Collections.singleton("flag");
        }
    }
}
