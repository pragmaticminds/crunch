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

package org.pragmaticminds.crunch.runtime.sort;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.values.UntypedValues;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the value assigner.
 *
 * @author julian
 * Created by julian on 03.11.17
 */
public class ValueEventAssignerTest {

    private ValueEventAssigner assigner;

    @Before
    public void setUp() {
        assigner = new ValueEventAssigner(100);
    }

    @Test
    public void extractTimestamp_works() {
        long extractTimestamp = assigner.extractTimestamp(UntypedValues.builder().timestamp(10L).build(), -1);

        assertEquals(10L, extractTimestamp);
    }

    @Test
    public void checkAndGetNextWatermark_works() {
        UntypedValues event = UntypedValues.builder().timestamp(10L).build();
        assigner.extractTimestamp(event, -1);
        Watermark watermark = assigner.checkAndGetNextWatermark(event, event.getTimestamp());

        assertEquals(-90, watermark.getTimestamp());
    }
}